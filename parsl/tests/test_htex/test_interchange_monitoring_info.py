from unittest import mock

import pytest

from parsl.executors.high_throughput.interchange import Interchange
from parsl.executors.high_throughput.manager_record import ManagerRecord
from parsl.monitoring.message_type import MessageType


@pytest.mark.local
def test_send_monitoring_info_maps_python_version():
    # internally we use python_version, but the monitoring table uses _v
    ix = mock.Mock(spec=Interchange, run_id="test_run_id")
    radio = mock.Mock()
    rec = ManagerRecord(last_heartbeat=0.0, python_version="3.12.4")

    Interchange._send_monitoring_info(ix, radio, rec)

    (mtype, msg), = radio.send.call_args.args
    assert mtype == MessageType.NODE_INFO
    assert msg["python_v"] == "3.12.4"
