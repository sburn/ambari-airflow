#!/usr/bin/env python
import sys, os, pwd, grp, signal, time, base64
from resource_management import *
from resource_management.core.exceptions import Fail
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, Directory, File
from resource_management.core.shell import call
from resource_management.core.system import System
from resource_management.core import sudo
from resource_management.core.shell import as_sudo
from resource_management.libraries.functions.default import default
from resource_management.core.resources import File

def airflow_make_systemd_scripts_scheduler(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
User={airflow_user}
Group={airflow_group}
Type=forking
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow scheduler -D --pid {airflow_scheduler_pid_file} --stderr {airflow_log_dir}/scheduler-err.log --stdout {airflow_log_dir}/scheduler-out.log -l {airflow_log_dir}/scheduler-log.log"
PIDFile={airflow_scheduler_pid_file}
Restart=on-failure
RestartSec=5s
PrivateTmp=False
StandardOutput=syslog+console
SyslogIdentifier=airflow-scheduler

[Install]
WantedBy=multi-user.target

""")

	File("/etc/systemd/system/airflow-scheduler.service",
	    mode=0644,
	    owner=params.airflow_user,
	    group=params.airflow_group,
	    content=confFileText
	)

	Execute(('systemctl', 'daemon-reload'),
	    sudo=True)


def airflow_make_systemd_scripts_webserver(env):
	import params
	env.set_params(params)

	# Create Airflow webserver systemd service
	confFileText = format("""[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
User={airflow_user}
Group={airflow_group}
Type=forking
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow webserver -D --pid {airflow_webserver_pid_file} --stderr {airflow_log_dir}/webserver-err.log --stdout {airflow_log_dir}/webserver-out.log -l {airflow_log_dir}/webserver-log.log"
PIDFile={airflow_webserver_pid_file}
Restart=on-failure
RestartSec=5s
PrivateTmp=False
StandardOutput=syslog+console
SyslogIdentifier=airflow-webserver

[Install]
WantedBy=multi-user.target
""")

	File("/etc/systemd/system/airflow-webserver.service",
	    mode=0644,
	    owner=params.airflow_user,
	    group=params.airflow_group,
	    content=confFileText
	)

	# Create Airflow Celery Flower systemd service
	confFileText = format("""[Unit]
Description=Airflow Celery Flower
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
User={airflow_user}
Group={airflow_group}
Type=forking
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow flower -D --pid {airflow_flower_pid_file} --stderr {airflow_log_dir}/flower-err.log --stdout {airflow_log_dir}/flower-out.log -l {airflow_log_dir}/flower-log.log"
PIDFile={airflow_flower_pid_file}
Restart=on-failure
RestartSec=5s
PrivateTmp=False
StandardOutput=syslog+console
SyslogIdentifier=airflow-flower

[Install]
WantedBy=multi-user.target
""")

	File("/etc/systemd/system/airflow-flower.service",
	    mode=0644,
	    owner=params.airflow_user,
	    group=params.airflow_group,
	    content=confFileText
	)


	# Apply changes to systemd
	Execute(('systemctl', 'daemon-reload'), 
	    sudo=True)


def airflow_make_systemd_scripts_worker(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow worker daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
User={airflow_user}
Group={airflow_group}
Type=forking
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow worker -D --pid {airflow_worker_pid_file} --stderr {airflow_log_dir}/worker-err.log --stdout {airflow_log_dir}/worker-out.log -l {airflow_log_dir}/worker-log.log"
PIDFile={airflow_worker_pid_file}
Restart=on-failure
RestartSec=5s
PrivateTmp=False
StandardOutput=syslog+console
SyslogIdentifier=airflow-worker

[Install]
WantedBy=multi-user.target

""")

	File("/etc/systemd/system/airflow-worker.service",
	    mode=0644,
	    owner=params.airflow_user,
	    group=params.airflow_group,
	    content=confFileText
	)

	Execute(('systemctl', 'daemon-reload'),
	    sudo=True)


def airflow_generate_config_for_section(sections):
	"""
	Generating values for airflow.cfg for each section.
	This allows to add custom-site configuration from ambari to cfg file.
	"""
	result = {}
	for section, data in sections.items():
		section_config = ""
		for key, value in data.items():
			section_config += format("{key} = {value}\n")
		result[section] = section_config
	return result


def airflow_configure(env):
	import params
	env.set_params(params)

	## Create directories

	Directory(
	    [params.airflow_home, params.airflow_pid_dir, params.airflow_log_dir],
	    mode=0755,
	    cd_access='a',
	    owner=params.airflow_user,
	    group=params.airflow_group,
	    create_parents=True,
	    recursive_ownership=True
	)

	## Write config file

	airflow_config_file = ""

	airflow_config = airflow_generate_config_for_section({
		"core" : params.config['configurations']['airflow-core-site'],
		"cli" : params.config['configurations']['airflow-cli-site'],
		"api" : params.config['configurations']['airflow-api-site'],
		"operators" : params.config['configurations']['airflow-operators-site'],
		"webserver" : params.config['configurations']['airflow-webserver-site'],
		"email" : params.config['configurations']['airflow-email-site'],
		"smtp" : params.config['configurations']['airflow-smtp-site'],
		"celery" : params.config['configurations']['airflow-celery-site'],
		"dask" : params.config['configurations']['airflow-dask-site'],
		"scheduler" : params.config['configurations']['airflow-scheduler-site'],
		"ldap" : params.config['configurations']['airflow-ldap-site'],
		"mesos" : params.config['configurations']['airflow-mesos-site'],
		"kerberos" : params.config['configurations']['airflow-kerberos-site'],
		"github_enterprise" : params.config['configurations']['airflow-githubenterprise-site'],
		"admin" : params.config['configurations']['airflow-admin-site'],
		"lineage" : params.config['configurations']['airflow-lineage-site'],
		"atlas" : params.config['configurations']['airflow-atlas-site'],
		"hive" : params.config['configurations']['airflow-hive-site'],
		"celery_broker_transport_options" : params.config['configurations']['airflow-celerybrokertransportoptions-site'],
		"elasticsearch" : params.config['configurations']['airflow-elasticsearch-site'],
		"kubernetes" : params.config['configurations']['airflow-kubernetes-site'],
		"kubernetes_secrets" : params.config['configurations']['airflow-kubernetessecrets-site']
	})

	for section, value in sorted(airflow_config.items()):
		airflow_config_file += format("[{section}]\n{value}\n")

	File(params.airflow_home + '/airflow.cfg',
	    mode=0644,
	    owner=params.airflow_user,
	    group=params.airflow_group,
	    content=airflow_config_file
	)


