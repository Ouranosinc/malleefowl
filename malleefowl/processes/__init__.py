from .wps_esgsearch import ESGSearchProcess
from .wps_download import Download
from .wps_thredds import ThreddsDownload
from .wps_workflow import DispelWorkflow
from .wps_custom_workflow import DispelCustomWorkflow

processes = [
    ESGSearchProcess(),
    Download(),
    ThreddsDownload(),
    DispelWorkflow(),
    DispelCustomWorkflow(),
]
