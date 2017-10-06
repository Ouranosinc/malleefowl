import json
import requests
import Cookie
from malleefowl import config


class AuthZ:
    def __init__(self, request):
        self.thredds_svc = None
        self.url = config.authz_url().strip('/')
        self.session = requests.Session()
        try:
            self.session.cookies.set('auth_tkt', request.cookies['auth_tkt'])
        except KeyError:
            # No token... will be anonymous
            pass

        response = self.session.get(self.url + '/services')
        if response.status_code != 200:
            raise response.raise_for_status()

        # Looking for the proper thredds service name base on our configured thredds server
        thredds_url = config.thredds_url().strip('/')
        try:
            services = json.loads(response.text)
            for key, service in services['services']['thredds'].items():
                if service['service_url'] in thredds_url:
                    self.thredds_svc = service['service_name']
                    break
        except:
            pass

    def is_auth(self, location, permission):
        if not self.thredds_svc:
            return False

        response = self.session.get(self.url + '/users/current/services/{svc}/resources'.
                                    format(svc=self.thredds_svc))
        if response.status_code != 200:
            raise response.raise_for_status()

        response_data = json.loads(response.text)

        service = response_data['service']
        if permission in service['permission_names']:
            # The user has global upload permission to the service
            return True

        # Support only one catalog per thredds and suppose that the catalog content is located under persist_path
        # If more than one catalog is given the permission will be search for each of them with the same location
        for c_id, catalog in service['resources'].items():
            for resource_path in self.tree_parser(catalog, permission):
                if location.startswith(resource_path.strip('/')):
                    return True
        return False

    def tree_parser(self, resources_tree, permission, root='', part_of_path=False):
        if part_of_path:
            root = '/'.join([root, resources_tree['resource_name']])
        if permission in resources_tree['permission_names']:
            yield root

        for r_id, resource in resources_tree['children'].items():
            for resource_path in self.tree_parser(resource, permission, root, part_of_path=True):
                yield resource_path
