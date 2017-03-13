Name:		nagios-plugins-eudat-b2find
Version:	2.2
Release:	0
Summary:	Nagios B2FIND probes
License:	GPLv3+
Packager:	Heinrich Widmann <widmann@dkrz.de>

Group:		Application
URL:		http://www.eudat.eu/b2find
BuildArch:	noarch
#Source0:	
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

#BuildRequires:	
Requires:	python
Requires:	python-argparse
#Requires:	python-json
#Requires:	python-urllib
#Requires:	python-urllib2

%description
This nagios plugin provides the nessecary script to check search functionality of the B2FIND discovery service b2find.eudat.eu .

%define _whoami %(whoami)
%define _b2findhomepackaging %(pwd)
%define _b2findNagiosPackage /usr/libexec/argo-monitoring/probes/eudat-b2find

# get all our source code in the $RPM_SOURCE_DIR
%prep
echo "The spec file directory is %{_b2findhomepackaging}"
echo "The user that built this is %{_whoami}"
# create string where git repo is started..
workingdir=`pwd`
cd %{_b2findhomepackaging}
b2findhome=`pwd`
cd $workingdir
# empty source directory and copy new files
rm -rf $RPM_SOURCE_DIR/*
cp -ar $b2findhome/* $RPM_SOURCE_DIR

# build images. We don't have to make them so exit
%build
exit 0

# put images in correct place
%install
rm -rf %{buildroot}
mkdir -p $RPM_BUILD_ROOT%{_b2findNagiosPackage}

cp $RPM_SOURCE_DIR/*.py         $RPM_BUILD_ROOT%{_b2findNagiosPackage}

# cleanup
%clean
rm -rf %{buildroot}

#provide files to rpm. Set attributes 
%files
# default attributes
%defattr(-,-,-,-)
# files
#include files
%{_b2findNagiosPackage}
# attributes on files and directory's
%attr(-,nagios,nagios)   %{_b2findNagiosPackage}
%attr(750,nagios,nagios) %{_b2findNagiosPackage}/*.py
%doc

%post
%changelog
* Tue Mar 07 2017  Heinrich Widmann <widmann@dkrz.de> 0.1
- Initial version of b2find nagios plugin
