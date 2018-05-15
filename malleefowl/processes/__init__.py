from .wps_esgsearch import ESGSearchProcess
from .wps_download import Download
from .wps_persist import Persist
from .wps_visualize import Visualize
from .wps_thredds import ThreddsDownload, ThreddsUrls, ThreddsOpenDapUrls
from .wps_workflow import DispelWorkflow
from .wps_custom_workflow import DispelCustomWorkflow

processes = [
    ESGSearchProcess(),
    Download(),
    Persist(),
    Visualize(),
    ThreddsDownload(),
    ThreddsUrls(),
    ThreddsOpenDapUrls(),
    DispelWorkflow(),
    DispelCustomWorkflow(),
]
