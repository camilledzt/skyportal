import base64
import functools
import json
import os
import tarfile
import tempfile
import traceback
from datetime import datetime, timedelta

import pandas as pd
import requests
import sqlalchemy as sa
from astropy.time import Time
from sqlalchemy.orm import scoped_session, sessionmaker
from swifttools.swift_too import Data, ObsQuery, Swift_TOO, UVOT_mode
from swifttools.xrt_prods import XRTProductRequest
from tornado.ioloop import IOLoop

from baselayer.app.env import load_env
from baselayer.app.flow import Flow
from baselayer.log import make_log

from ..utils import http
from . import MMAAPI, FollowUpAPI

env, cfg = load_env()


# Submission URL
API_URL = f"{cfg['app.swift.protocol']}://{cfg['app.swift.host']}:{cfg['app.swift.port']}/toop/submit_api.php"
XRT_URL = f"{cfg['app.swift_xrt_endpoint']}/run_userobject.php"

log = make_log("facility_apis/swift")


modes = {
    "0x9999": "0x9999 - Default (Filter of the day)",
    "0x30ed": "0x30ed - U+B+V+All UV",
    "0x223f": "0x223f - U+B+V+All UV (UV weighted, SN Mode)",
    "0x0270": "0x0270 - U+B+V+All UV (ToO Upload Mode)",
    "0x209a": "0x209a - U+B+V",
    "0x308f": "0x308f - All UV",
    "0x2019": "0x2019 - White",
    "0x018c": "0x018c - UVW1",
    "0x011e": "0x011e - UVW2",
    "0x015a": "0x015a - UVM2",
    "0x01aa": "0x01aa - U band",
    "0x2016": "0x2016 - B band",
    "0x2005": "0x2005 - V band",
    "0x122f": "0x122f - Grism 1 (UV)",
    "0x1230": "0x1230 - Grism 2 (Visible)",
    "0x0ff3": "0x0ff3 - Blocked (in case of too bright star)",
}

modes_keys, modes_values = zip(*modes.items())


class UVOTXRTMMAAPI(MMAAPI):
    """An interface to Swift MMA operations."""

    @staticmethod
    def retrieve(allocation, start_date, end_date):
        """Retrieve executed observations by Swift.

        Parameters
        ----------
        allocation : skyportal.models.Allocation
            The allocation with queue information.
        start_date : datetime.datetime
            Minimum time for observation request
        end_date : datetime.datetime
            Maximum time for observation request
        """

        altdata = allocation.altdata
        if not altdata:
            raise ValueError("Missing allocation information.")

        request_start = Time(start_date, format="datetime")
        request_end = Time(end_date, format="datetime")

        if request_start > request_end:
            raise ValueError("start_date must be before end_date.")

        fetch_obs = functools.partial(
            fetch_observations,
            allocation.instrument.id,
            request_start,
            request_end,
        )

        IOLoop.current().run_in_executor(None, fetch_obs)

    form_json_schema_altdata = {
        "type": "object",
        "properties": {
            "username": {"type": "string", "title": "Username"},
            "secret": {"type": "string", "title": "Secret"},
            "XRT_UserID": {"type": "string", "title": "XRT User ID"},
        },
    }


def fetch_observations(instrument_id, request_start, request_end):
    """Fetch executed observations from the Swift API.
    instrument_id : int
        ID of the instrument
    request_start : astropy.time.Time
        Start time for the request.
    request_end : astropy.time.Time
        End time for the request.
    """

    oq = ObsQuery(begin=request_start.iso, end=request_end.iso)

    observations = []
    for row in oq:
        mode = UVOT_mode(row.uvot)
        if mode.entries is None:
            continue
        # each observation actually cycles through filters
        # we will leave the download for a query from an
        # individual source page
        if (row.ra_object is None) or (row.dec_object is None):
            continue
        filt = f"uvot::{mode.entries[0].filter_name}"
        observation = {
            "observation_id": int(row.obsid),
            "obstime": row.begin,
            "RA": row.ra_object,
            "Dec": row.dec_object,
            "seeing": None,
            "limmag": None,
            "exposure_time": row.exposure.seconds,
            "filter": filt,
            "processed_fraction": 1.0,
            "target_name": row.targname,
        }

        observations.append(observation)
    obstable = pd.DataFrame.from_dict(observations)

    from skyportal.handlers.api.observation import add_observations

    add_observations(instrument_id, obstable)


class UVOTXRTRequest:
    """A JSON structure for Swift UVOT/XRT requests."""

    def __init__(self, request):
        """Initialize UVOT/XRT request.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.

        """

        self.requestgroup = self._build_payload(request)

    def _build_payload(self, request):
        """Payload json for Swift UVOT/XRT TOO requests.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.

        Returns
        -------
        payload : swifttools.swift_too.Swift_TOO
            payload for requests.
        """

        altdata = request.allocation.altdata

        too = Swift_TOO()
        too.username = altdata["username"]
        too.shared_secret = altdata["secret"]

        too.source_name = request.obj.id
        too.ra, too.dec = request.obj.ra, request.obj.dec

        too.source_type = request.payload["source_type"]

        too.exp_time_per_visit = request.payload["exposure_time"]
        too.monitoring_freq = f"{request.payload['monitoring_freq']:d} days"
        too.num_of_visits = int(request.payload["exposure_counts"])

        too.opt_mag = request.payload.get("opt_mag", None)
        too.opt_filt = request.payload.get("opt_filt", None)
        too.xrt_countrate = request.payload.get("xrt_countrate", None)
        too.exp_time_just = request.payload["exp_time_just"]
        too.immediate_objective = request.payload["immediate_objective"]

        try:
            too.urgency = int(request.payload["urgency"])
        except Exception as e:
            raise ValueError(f"Could not convert urgency to a valid integer: {e}")
        if too.urgency < 0 or too.urgency > 4:
            raise ValueError(
                f"urgency must be one of 0, 1, 2, 3, or 4, and not: {too.urgency}"
            )
        if request.payload["obs_type"] not in [
            "Spectroscopy",
            "Light Curve",
            "Position",
            "Timing",
        ]:
            raise ValueError("obs_type not an allowed value.")
        too.obs_type = request.payload["obs_type"]

        modes_index = modes_values.index(request.payload["uvot_mode"])
        too.uvot_mode = modes_keys[modes_index]
        too.science_just = request.payload["science_just"]
        if too.uvot_mode != "0x9999":
            too.uvot_just = request.payload.get("uvot_just", None)
            if not too.uvot_just:
                raise ValueError(
                    "uvot_just is required when the UVOT mode select is 0x0270 (ToO Upload Mode)."
                )

        return too


class XRTAPIRequest:
    """A JSON structure for Swift XRT API requests."""

    def __init__(self, request):
        """Initialize XRT API request.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.

        """

        self.requestgroup = self._build_payload(request)

    def _build_payload(self, request):
        """Payload json for Swift UVOT/XRT TOO requests.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.

        Returns
        -------
        payload : swifttools.swift_too.Swift_TOO
            payload for requests.
        """

        altdata = request.allocation.altdata

        T0 = Time(request.payload["T0"], format="iso")
        MET = Time("2001-01-01 00:00:00", format="iso")
        Tdiff = (T0 - MET).jd * 86400

        centroid = bool(request.payload.get("detornot", False))

        myReq = XRTProductRequest(altdata["XRT_UserID"])
        myReq.setGlobalPars(
            getTargs=True,
            centroid=centroid,
            name=request.obj.id,
            RA=request.obj.ra,
            Dec=request.obj.dec,
            centMeth=request.payload["centMeth"],
            detMeth=request.payload["detMeth"],
            useSXPS=False,
            T0=Tdiff,
            posErr=request.payload["poserr"],
        )
        myReq.addLightCurve(binMeth="counts", pcCounts=20, wtCounts=30, dynamic=True)
        myReq.addSpectrum(hasRedshift=False)
        myReq.addStandardPos()
        myReq.addEnhancedPos()
        myReq.addAstromPos(useAllObs=True)

        return myReq


class UVOTXRTBATDataRequest:
    """A JSON structure for Swift UVOT/XRT/BAT Data requests."""

    def __init__(self, request):
        """Initialize UVOT/XRT/BAT data request.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.

        """

        self.requestgroup = self._build_payload(request)

    def _build_payload(self, request):
        """Payload json for Swift UVOT/XRT/BAT data requests.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.

        Returns
        -------
        payload : swifttools.swift_too.ObsQuery
            payload for requests.
        """

        oq = ObsQuery(
            ra=request.obj.ra,
            dec=request.obj.dec,
            radius=5.0 / 60,
            begin=request.payload["start_date"],
            end=request.payload["end_date"],
        )

        return oq


def download_observations(request_id, oq):
    """Fetch data from the Swift API.
    request_id : int
        SkyPortal ID for request
    oq : swifttools.swift_too.ObsQuery
        Swift observation query
    """

    from ..models import Comment, DBSession, FollowupRequest, Group

    Session = scoped_session(sessionmaker())
    if Session.registry.has():
        session = Session()
    else:
        session = Session(bind=DBSession.session_factory.kw["bind"])

    try:
        req = session.scalars(
            sa.select(FollowupRequest).where(FollowupRequest.id == request_id)
        ).first()

        group_ids = [g.id for g in req.requester.accessible_groups]
        groups = session.scalars(
            Group.select(req.requester).where(Group.id.in_(group_ids))
        ).all()

        with tempfile.TemporaryDirectory() as tmpdirname:
            obsids = list({row.obsid for row in oq})
            for obsid in obsids:
                data = Data()
                data.obsid = obsid
                data.xrt = req.payload.get("XRT", False)
                data.uvot = req.payload.get("UVOT", False)
                data.bat = req.payload.get("BAT", False)
                data.outdir = tmpdirname

                if data.submit():
                    data.outdir = os.path.expanduser(data.outdir)
                    data.outdir = os.path.expandvars(data.outdir)
                    data.outdir = os.path.abspath(data.outdir)

                    # Index any existing files
                    for i in range(len(data.entries)):
                        fullfilepath = os.path.join(
                            data.outdir, data.entries[i].path, data.entries[i].filename
                        )
                        if os.path.exists(fullfilepath):
                            data.entries[i].localpath = fullfilepath
                    topdir = os.path.join(data.outdir, str(obsid))
                    if not os.path.isdir(topdir):
                        os.makedirs(topdir)

                    for dfile in data.entries:
                        if not dfile.download(outdir=data.outdir):
                            raise ValueError(f"Error downloading {dfile.filename}")
                    filename = os.path.join(tmpdirname, f"{obsid}.tar.gz")
                    with tarfile.open(filename, "w:gz") as tar:
                        tar.add(topdir, arcname=os.path.basename(topdir))

                    attachment_name = filename.split("/")[-1]
                    with open(filename, "rb") as f:
                        attachment_bytes = base64.b64encode(f.read())
                    comment = Comment(
                        text=f"Swift Data: {obsid}",
                        obj_id=req.obj.id,
                        attachment_bytes=attachment_bytes,
                        attachment_name=attachment_name,
                        author=req.requester,
                        groups=groups,
                        bot=True,
                    )
                    session.add(comment)
        req.status = "Result posted as comment"
        session.commit()

    except Exception as e:
        session.rollback()
        log(f"Unable to post data for {request_id}: {e}")
    finally:
        session.close()
        Session.remove()


class UVOTXRTAPI(FollowUpAPI):
    """An interface to Swift operations."""

    @staticmethod
    def get(request, session, **kwargs):
        """Get an analysis request result from Swift.

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to retrieve Swift XRT data.
        session : baselayer.DBSession
            Database session to use for photometry
        """

        from ..models import Comment, FollowupRequest, Group

        req = (
            session.query(FollowupRequest)
            .filter(FollowupRequest.id == request.id)
            .one()
        )

        altdata = request.allocation.altdata

        if not altdata:
            raise ValueError("Missing allocation information.")

        if request.payload["request_type"] == "XRT API":
            content = req.transactions[-1].response["content"]
            content = json.loads(content)
            swiftreq = XRTAPIRequest(request)

            swiftreq.requestgroup._status = 1
            swiftreq.requestgroup._submitted = int(content["OK"])
            swiftreq.requestgroup._jobID = content["JobID"]
            swiftreq.requestgroup._retData["URL"] = content["URL"]
            swiftreq.requestgroup._retData["jobPars"] = content["jobPars"]

            if not swiftreq.requestgroup.complete:
                raise ValueError("Result not yet available. Please try again later.")
            else:
                group_ids = [g.id for g in request.requester.accessible_groups]
                groups = session.scalars(
                    Group.select(request.requester).where(Group.id.in_(group_ids))
                ).all()
                with tempfile.TemporaryDirectory() as tmpdirname:
                    retDict = swiftreq.requestgroup.downloadProducts(tmpdirname)
                    for key in retDict:
                        filename = retDict[key]
                        attachment_name = filename.split("/")[-1]
                        with open(filename, "rb") as f:
                            attachment_bytes = base64.b64encode(f.read())
                        comment = Comment(
                            text=f"Swift XRT: {key}",
                            obj_id=request.obj.id,
                            attachment_bytes=attachment_bytes,
                            attachment_name=attachment_name,
                            author=request.requester,
                            groups=groups,
                            bot=False,
                        )
                        session.add(comment)
                    req.status = "Result posted as comment"
                    session.commit()
        elif request.payload["request_type"] == "XRT/UVOT/BAT Data":
            swiftreq = UVOTXRTBATDataRequest(request)

            download_obs = functools.partial(
                download_observations,
                req.id,
                swiftreq.requestgroup,
            )

            IOLoop.current().run_in_executor(None, download_obs)

        if kwargs.get("refresh_source", False):
            flow = Flow()
            flow.push(
                "*",
                "skyportal/REFRESH_SOURCE",
                payload={"obj_key": request.obj.internal_key},
            )
        if kwargs.get("refresh_requests", False):
            flow = Flow()
            flow.push(
                request.last_modified_by_id,
                "skyportal/REFRESH_FOLLOWUP_REQUESTS",
            )

    # subclasses *must* implement the method below
    @staticmethod
    def submit(request, session, **kwargs):
        """Submit a follow-up request to Swift's UVOT/XRT

        Parameters
        ----------
        request : skyportal.models.FollowupRequest
            The request to add to the queue and the SkyPortal database.
        session: sqlalchemy.Session
            Database session for this transaction
        """

        from ..models import FacilityTransaction

        altdata = request.allocation.altdata
        if not altdata:
            raise ValueError("Missing allocation information.")

        if request.payload["request_type"] == "XRT/UVOT ToO":
            swiftreq = UVOTXRTRequest(request)
            swiftreq.requestgroup.validate()

            r = requests.post(
                url=API_URL, verify=True, data={"jwt": swiftreq.requestgroup.jwt}
            )

            if r.status_code == 200:
                request.status = "submitted"
            else:
                request.status = f"rejected: {r.content}"
                log(
                    f"Failed to submit Swift request for {request.id} (obj {request.obj.id}): {r.content}"
                )
                try:
                    flow = Flow()
                    flow.push(
                        request.last_modified_by_id,
                        "baselayer/SHOW_NOTIFICATION",
                        payload={
                            "note": f"Failed to submit Swift request: {r.content}",
                            "type": "error",
                        },
                    )
                except Exception as e:
                    log(f"Failed to send notification: {e}")

            transaction = FacilityTransaction(
                request=http.serialize_requests_request(r.request),
                response=http.serialize_requests_response(r),
                followup_request=request,
                initiator_id=request.last_modified_by_id,
            )

            session.add(transaction)

        elif request.payload["request_type"] == "XRT API":
            swiftreq = XRTAPIRequest(request)
            r = requests.post(url=XRT_URL, json=swiftreq.requestgroup.getJSONDict())
            returnedData = json.loads(r.text)
            if r.status_code != 200:
                request.status = f"rejected: {r.reason}"
            else:
                if returnedData["OK"] == 0:
                    request.status = (
                        f"rejected: {returnedData['ERROR'], returnedData['listErr']}"
                    )
                else:
                    request.status = "submitted"

            transaction = FacilityTransaction(
                request=http.serialize_requests_request(r.request),
                response=http.serialize_requests_response(r),
                followup_request=request,
                initiator_id=request.last_modified_by_id,
            )

            session.add(transaction)

        elif request.payload["request_type"] == "XRT/UVOT/BAT Data":
            swiftreq = UVOTXRTBATDataRequest(request)
            request.status = f"Number of observations: {len(swiftreq.requestgroup)}"
        else:
            raise ValueError("Invalid request type.")

        if kwargs.get("refresh_source", False):
            flow = Flow()
            flow.push(
                "*",
                "skyportal/REFRESH_SOURCE",
                payload={"obj_key": request.obj.internal_key},
            )
        if kwargs.get("refresh_requests", False):
            flow = Flow()
            flow.push(
                request.last_modified_by_id,
                "skyportal/REFRESH_FOLLOWUP_REQUESTS",
            )

        try:
            notification_type = request.allocation.altdata.get(
                "notification_type", "none"
            )
            if notification_type == "slack":
                from ..utils.notifications import request_notify_by_slack

                request_notify_by_slack(
                    request,
                    session,
                    is_update=False,
                )
            elif notification_type == "email":
                from ..utils.notifications import request_notify_by_email

                request_notify_by_email(
                    request,
                    session,
                    is_update=False,
                )
        except Exception as e:
            traceback.print_exc()
            log(f"Error sending notification: {e}")

    form_json_schema = {
        "type": "object",
        "properties": {
            "request_type": {
                "type": "string",
                "enum": ["XRT/UVOT/BAT Data", "XRT/UVOT ToO", "XRT API"],
                "default": "XRT/UVOT/BAT Data",
                "title": "Request Type",
            },
        },
        "dependencies": {
            "request_type": {
                "oneOf": [
                    {
                        "properties": {
                            "request_type": {
                                "enum": ["XRT/UVOT/BAT Data"],
                            },
                            "start_date": {
                                "type": "string",
                                "default": str(
                                    datetime.utcnow() - timedelta(days=365)
                                ).replace("T", ""),
                                "title": "Start Date (UT)",
                            },
                            "end_date": {
                                "type": "string",
                                "title": "End Date (UT)",
                                "default": str(datetime.utcnow()).replace("T", ""),
                            },
                            "XRT": {
                                "title": "Do you want XRT data?",
                                "type": "boolean",
                            },
                            "UVOT": {
                                "title": "Do you want UVOT data?",
                                "type": "boolean",
                            },
                            "BAT": {
                                "title": "Do you want BAT data?",
                                "type": "boolean",
                            },
                        }
                    },
                    {
                        "properties": {
                            "request_type": {
                                "enum": ["XRT API"],
                            },
                            "detornot": {
                                "title": "Do you want to centroid?",
                                "type": "boolean",
                            },
                            "centMeth": {
                                "type": "string",
                                "enum": ["simple", "iterative"],
                                "default": "simple",
                                "title": "Centroid Method",
                            },
                            "detMeth": {
                                "type": "string",
                                "enum": ["simple", "iterative"],
                                "default": "simple",
                                "title": "Detection Method",
                            },
                            "T0": {
                                "type": "string",
                                "title": "Date (UT)",
                                "default": str(datetime.utcnow()).replace("T", ""),
                            },
                            "poserr": {
                                "title": "Position Error [arcmin]",
                                "type": "number",
                                "default": 1,
                            },
                            "binMeth": {
                                "type": "string",
                                "enum": ["counts", "time", "snapshot", "obsid"],
                                "default": "counts",
                                "title": "Binning method",
                            },
                        }
                    },
                    {
                        "properties": {
                            "request_type": {
                                "enum": ["XRT/UVOT ToO"],
                            },
                            "exposure_time": {
                                "title": "Exposure Time per visit [s]",
                                "type": "number",
                                "default": 4000.0,
                            },
                            "exposure_counts": {
                                "title": "Number of visits",
                                "type": "number",
                                "default": 1,
                                "minimum": 1,
                            },
                            "monitoring_freq": {
                                "title": "Monitoring Frequency [day]",
                                "type": "number",
                                "default": 1,
                            },
                            "opt_mag": {
                                "title": "Optical Magnitude",
                                "type": "number",
                            },
                            "opt_filt": {
                                "title": "Optical Filter",
                                "type": "string",
                            },
                            "xrt_countrate": {
                                "title": "XRT Count rate [counts/s]",
                                "type": "number",
                                "default": 0.0025,
                            },
                            "urgency": {
                                "type": "string",
                                "enum": ["1", "2", "3", "4"],
                                "default": "3",
                                "title": "Urgency: (1) Within 4 hours. (2) Within the next 24 hours. (3) In the next few days. (4) Weeks to a month.",
                            },
                            "obs_type": {
                                "type": "string",
                                "enum": [
                                    "Spectroscopy",
                                    "Light Curve",
                                    "Position",
                                    "Timing",
                                ],
                                "default": "Light Curve",
                                "title": "Observation Type",
                            },
                            "source_type": {
                                "title": "Source Type",
                                "type": "string",
                                "default": "Optical fast transient",
                            },
                            "exp_time_just": {
                                "title": "Exposure Time Justification",
                                "type": "string",
                                "default": "At ~2.5e-3 counts/sec, 4ks should suffice to achieve a high SNR, assuming a background of ~1e-4 counts/sec (Pagani et al. 2007)",
                            },
                            "immediate_objective": {
                                "title": "Immediate Objective",
                                "type": "string",
                                "default": "We wish to measure the X-ray emission of an optically discovered potential orphan afterglow/kilonova.",
                            },
                            "uvot_mode": {
                                "title": "UVOT Mode",
                                "type": "string",
                                "enum": list(modes_values),
                                "default": "0x9999 - Default (Filter of the day)",
                            },
                            "science_just": {
                                "title": "Science Justification",
                                "type": "string",
                                "default": "An X-ray detection of this transient will further associate this object to a relativistic explosion and will help unveil the nature of the progenitor type.",
                            },
                        },
                        "dependencies": {
                            "uvot_mode": {
                                "oneOf": [
                                    {
                                        "properties": {
                                            "uvot_mode": {
                                                "enum": [
                                                    "0x9999 - Default (Filter of the day)"
                                                ],
                                            },
                                        }
                                    },
                                    {
                                        "properties": {
                                            "uvot_mode": {
                                                "not": {
                                                    "enum": [
                                                        "0x9999 - Default (Filter of the day)"
                                                    ],
                                                },
                                            },
                                            "uvot_just": {
                                                "title": "UVOT Mode Justification",
                                                "type": "string",
                                                "default": "We wish to map the entire transient SED in all UV filters.",
                                            },
                                        },
                                        "required": ["uvot_just"],
                                    },
                                ]
                            },
                        },
                    },
                ],
            },
        },
    }

    form_json_schema_altdata = {
        "type": "object",
        "properties": {
            "username": {"type": "string", "title": "Username"},
            "secret": {"type": "string", "title": "Secret"},
            "XRT_UserID": {"type": "string", "title": "XRT User ID"},
            "notification_type": {
                "type": "string",
                "title": "Notification Type",
                "enum": ["none", "slack", "email"],
            },
        },
        "dependencies": {
            "notification_type": {
                "oneOf": [
                    {
                        "properties": {
                            "notification_type": {"enum": ["none"]},
                        },
                    },
                    {
                        "properties": {
                            "notification_type": {"enum": ["slack"]},
                            "slack_workspace": {
                                "type": "string",
                                "title": "Slack Workspace",
                            },
                            "slack_channel": {
                                "type": "string",
                                "title": "Slack Channel",
                            },
                            "slack_token": {
                                "type": "string",
                                "title": "Slack Token",
                            },
                            "include_comments": {
                                "type": "boolean",
                                "title": "Include Comments",
                                "default": False,
                            },
                        },
                        "required": [
                            "slack_workspace",
                            "slack_channel",
                            "slack_token",
                        ],
                    },
                    {
                        "properties": {
                            "notification_type": {"enum": ["email"]},
                            "email": {
                                "type": "string",
                                "title": "Email",
                            },
                            "include_comments": {
                                "type": "boolean",
                                "title": "Include Comments",
                                "default": False,
                            },
                        },
                        "required": [
                            "email",
                        ],
                    },
                ]
            },
        },
    }

    ui_json_schema = {}

    priorityOrder = "desc"
