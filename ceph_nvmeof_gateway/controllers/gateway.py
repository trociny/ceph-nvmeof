import cherrypy
import logging
import socket
import sqlalchemy.exc

from . import BackendControllerRoute, EndpointDoc, FrontendControllerRoute, RESTController
from ceph_nvmeof_gateway.models.gateway import Gateway, GatewayPortal
from ceph_nvmeof_gateway.schemas.gateway import Gateway as GatewaySchema, \
    GatewayPortal as GatewayPortalSchema
from ceph_nvmeof_gateway.models.host import Host
from ceph_nvmeof_gateway.models.image import Image


logger = logging.getLogger(__name__)
gateway_schema = GatewaySchema()
gateway_schemas = GatewaySchema(many=True)

gateway_portal_schema = GatewayPortalSchema()
gateway_portal_schemas = GatewayPortalSchema(many=True)


@FrontendControllerRoute('/gateways')
class Gateways(RESTController):
    RESOURCE_ID = 'gateway_id'

    @EndpointDoc(responses={200: gateway_schemas})
    def list(self):
        return self.db.query(Gateway).all()

    @EndpointDoc(responses={200: gateway_schema})
    def get(self, gateway_id):
        return self.db.query(Gateway).get_or_404(gateway_id)

    @EndpointDoc(parameters=gateway_schema,
                 responses={200: gateway_schema})
    def create(self, gateway):
        logger.debug("create: gateway={}".format(gateway_schema.dumps(gateway)))

        self.db.add(gateway)
        try:
            self.db.commit()
        except sqlalchemy.exc.IntegrityError:
            self.db.rollback()
            raise cherrypy.HTTPError(422, message='duplicate gateway')

        return gateway

    def delete(self, gateway_id):
        gateway = self.db.query(Gateway).get_or_404(gateway_id)
        self.db.delete(gateway)


@FrontendControllerRoute('/gateways/{gateway_id}/portals')
class GatewayPortals(RESTController):
    RESOURCE_ID = 'gateway_portal_id'

    @EndpointDoc(responses={200: gateway_portal_schemas})
    def list(self, gateway_id):
        self.db.query(Gateway).get_or_404(gateway_id)
        return self.db.query(GatewayPortal).filter_by(gateway_id=gateway_id)

    @EndpointDoc(responses={200: gateway_portal_schema})
    def get(self, gateway_id, gateway_portal_id):
        self.db.query(Gateway).get_or_404(gateway_id)
        return self.db.query(GatewayPortal).filter_by(id=gateway_portal_id,
                                                      gateway_id=gateway_id).first_or_404()

    @EndpointDoc(parameters=gateway_portal_schema,
                 responses={201: gateway_portal_schema})
    def create(self, gateway_id, gateway_portal):
        gateway = self.db.query(Gateway).get_or_404(gateway_id)

        host = self.db.query(Host).filter_by(
            id=gateway_portal.host_id).first_or_404()
        image = self.db.query(Image).get_or_404(host.images[0].id)

        gateway_portal.gateway = gateway
        self.db.add(gateway_portal)
        try:
            self.db.commit()
        except sqlalchemy.exc.IntegrityError:
            self.db.rollback()
            raise cherrypy.HTTPError(422, message='duplicate gateway portal')

        if gateway_portal.gateway.name == socket.getfqdn():
            try:
                self.nvmeof_target.create_bdev(image.id, image.image_spec)
                self.nvmeof_target.create_subsystem(
                    host.nqn, image.id, gateway_portal.transport_type,
                    gateway_portal.address, gateway_portal.port)
            except Exception as e:
                raise cherrypy.HTTPError(422, message="{}".format(e))

        return gateway_portal

    def delete(self, gateway_id, gateway_portal_id):
        gateway_portal = self.db.query(GatewayPortal).filter_by(
            id=gateway_portal_id, gateway_id=gateway_id).first_or_404()
        self.db.delete(gateway_portal)

        host = self.db.query(Host).filter_by(
            id=gateway_portal.host_id).first_or_404()
        image = self.db.query(Image).get_or_404(host.images[0].id)

        if gateway_portal.gateway.name == socket.getfqdn():
            try:
                self.nvmeof_target.delete_subsystem(host.nqn)
                self.nvmeof_target.delete_bdev(image.id)
            except Exception as e:
                raise cherrypy.HTTPError(422, message="{}".format(e))

@BackendControllerRoute('/gateways')
class GatewaysBackend(RESTController):
    RESOURCE_ID = 'gateway_id'

    def create(self, gateway_spec):
        # TODO
        pass

    def delete(self, gateway_id):
        # TODO
        pass
