import logging
import os
import time
import subprocess

from sqlalchemy.orm import sessionmaker
from . import db
from ceph_nvmeof_gateway.models.gateway import Gateway, GatewayPortal
from ceph_nvmeof_gateway.models.image import Image
from ceph_nvmeof_gateway.models.host import Host, HostImages

logger = logging.getLogger(__name__)

class Target:
    def __init__(self, settings):
        self.settings = settings
        self.target = None
        self.images = {}
        self.hosts = {}
        self.host_images = {}
        self.gateways = {}
        self.portals = {}
        self.namespaces = {}

    def start(self):
        nvmf_tgt = self.settings.config.spdk_nvmf_tgt
        logger.info('spawning %s', nvmf_tgt)
        # TODO: make sure the process is always terminated
        self.target = subprocess.Popen(nvmf_tgt, start_new_session=True)
        logger.debug('pid %s', self.target.pid)

        time.sleep(3) # XXXMG: find a better way to wait for the target is ready for rpc
        opts = "-q 256 -p 18 -u 1048576 -i 1048576 -n 1023"
        cmd = "nvmf_create_transport -t TCP {}".format(opts)
        logger.debug('creating target tcp transport')
        self.rpc(cmd)

        self.init_from_db()

    def init_from_db(self):
        Session = sessionmaker()
        engine = db.init_engine(self.settings)
        Session.configure(bind=engine)
        session = Session()

        for image in session.query(Image):
            self.images[image.id] = image.image_spec

        for host in session.query(Host):
            self.hosts[host.id] = host.nqn

        # XXXMG: seems unnecessary: use host.images from query(Host) above
        for host_id, image_id in session.query(HostImages):
            if host_id not in self.hosts:
                logger.debug('unknown host id %s for image id %s',
                             host_id, image_id)
                continue
            if image_id not in self.images:
                logger.debug('unknown image id %s for host id %s',
                             image_id, host_id)
                continue
            self.host_images[host_id] = image_id

        for gateway in session.query(Gateway):
            self.gateways[gateway.id] = gateway.name

        for image_id, image_spec in self.images.items():
            ns = self.create_bdev(image_id, self.images[image_id])

        for portal in session.query(GatewayPortal):
            if portal.gateway_id not in self.gateways:
                logger.debug('unknown gateway id %s for portal %s',
                             portal.gateway_id, portal.id)
                continue
            if portal.host_id not in self.hosts:
                logger.debug('unknown host id %s for portal %s',
                             portal.host_id, portal.id)
                continue
            if portal.host_id not in self.host_images:
                logger.debug('no images for host id %s', portal.host_id)
                continue
            self.portals[portal.host_id] = portal

        for id, portal in self.portals.items():
            host_id = portal.host_id
            nqn = self.hosts[host_id]
            image_id = self.host_images[host_id]
            self.create_subsystem(nqn, image_id, portal.transport_type,
                                  portal.address, portal.port)

    def shut_down(self):
        if not self.target:
            return

        for id, portal in self.portals.items():
            host_id = portal.host_id
            nqn = self.hosts[host_id]
            image_id = self.host_images[host_id]
            self.delete_subsystem(nqn)
            self.delete_bdev(image_id)

        logger.info('terminating process %s', self.target.pid)
        self.target.kill()
        self.target.wait()
        self.target = None

    def rpc(self, cmd):
        rpc = self.settings.config.spdk_nvmf_rpc
        cmd = '{} {}'.format(rpc, cmd)
        logger.debug('%s', cmd)
        return subprocess.check_output(cmd, shell=True).decode('ascii')

    def create_bdev(self, image_id, image_spec):
        pool, image = image_spec.split('/')
        ns = self.rpc("bdev_rbd_create {} {} 4096".format(pool, image)).strip()
        logger.debug('image_id %s ns %s', image_id, ns)
        self.namespaces[image_id] = ns
        return ns

    def delete_bdev(self, image_id):
        logger.debug('image_id %s', image_id)
        ns = self.namespaces[image_id]
        self.rpc("bdev_rbd_delete {}".format(ns))
        del self.namespaces[image_id]

    def create_subsystem(self, nqn, image_id, transport_type, address, port):
        logger.debug('image_id %s', image_id)
        ns = self.namespaces[image_id]
        self.rpc("nvmf_create_subsystem {} -d SPDK-20 -a".format(nqn))
        self.rpc("nvmf_subsystem_add_ns {} {}".format(nqn, ns))
        self.rpc("nvmf_subsystem_add_listener {} -t {} -a {} -s {}".format(
            nqn, transport_type, address, port))

    def delete_subsystem(self, nqn):
        self.rpc("nvmf_delete_subsystem {}".format(nqn))
