Name:           ceph-nvmeof
Version:        0.1
Release:        1%{?dist}
Group:          System/Filesystems
Summary:        Python modules for Ceph NVMeoF gateway configuration management
License:        GPLv3+
URL:            https://github.com/ceph/ceph-nvmeof
Source0:        https://github.com/ceph/ceph-nvmeof/archive/%{version}/%{name}-%{version}.tar.gz

BuildArch:      noarch

Requires:       ceph-common >= 10.2.2
BuildRequires:  python3-devel
BuildRequires:  python3-setuptools
Requires:       python3-rados >= 10.2.2
Requires:       python3-rbd >= 10.2.2
BuildRequires:  python3-grpcio-tools
Requires:       python3-grpcio

%description
Python package providing the modules used to handle the configuration of an
NVMeoF gateway, backed by Ceph RBD.

%prep
%autosetup -p1

%build
./make_proto.sh $_PKGDEP_OPTS
%{py3_build}

%install
%{py3_install}


%files
%license LICENSE
%license COPYING
%doc README.md
%doc nvme_gw.config
%{python3_sitelib}/*
%{_bindir}/nvme_gw_server
%{_bindir}/nvme_gw_cli

%changelog
