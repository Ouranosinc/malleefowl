from pywps.Process import WPSProcess

from datetime import date

from malleefowl.process import show_status

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class MyProxyLogon(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "esgf_logon",
            title = "ESGF MyProxy Logon",
            version = "0.2",
            statusSupported=True,
            storeSupported=True)

        self.openid = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "For example: https://esgf-data.dkrz.de/esgf-idp/openid/username",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type(''),
            restrictedCharacters=[]
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="X509 Certificate",
            abstract="X509 Proxy Certificate",
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            asReference=True,
            )

        self.expires = self.addLiteralOutput(
            identifier="expires",
            title="Expires",
            abstract="Proxy Certificate will expire on given date",
            type=type(date(2014,4,8)),
            )

    def execute(self):
        from malleefowl.esgf import logon
        
        show_status(self, "start logon ...", 0)

        openid = self.openid.getValue()
        password = self.password.getValue()
        
        certfile = logon.myproxy_logon_with_openid(openid=openid, password=password, interactive=False)
        
        show_status(self, "logon successful", 100)

        self.output.setValue( certfile )
        self.expires.setValue(logon.cert_infos(certfile)['expires'])
        
