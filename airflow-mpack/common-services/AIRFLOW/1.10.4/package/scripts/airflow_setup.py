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
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow scheduler -D --pid ~/airflow/airflow-scheduler.pid"
PIDFile=/home/{airflow_user}/airflow/airflow-scheduler.pid
Restart=on-failure
RestartSec=5s
PrivateTmp=False

[Install]
WantedBy=multi-user.target

""")
	with open("/tmp/airflow-scheduler.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(('mv', '/tmp/airflow-scheduler.service', '/etc/systemd/system/airflow-scheduler.service'),
	    sudo=True
	)

	Execute(('systemctl', 'daemon-reload'),
	    sudo=True
	)


def airflow_make_systemd_scripts_webserver(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
User={airflow_user}
Group={airflow_group}
Type=forking
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow webserver -D --pid ~/airflow/airflow-webserver.pid"
PIDFile=/home/{airflow_user}/airflow/airflow-webserver.pid
Restart=on-failure
RestartSec=5s
PrivateTmp=False

[Install]
WantedBy=multi-user.target

""")
	with open("/tmp/airflow-webserver.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(('mv', '/tmp/airflow-webserver.service', '/etc/systemd/system/airflow-webserver.service'),
	    sudo=True
	)

	Execute(('systemctl', 'daemon-reload'),
	    sudo=True
	)


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
ExecStart=/bin/bash -c "source ~/venv-airflow/bin/activate && airflow worker -D --pid ~/airflow/airflow-worker.pid"
PIDFile=/home/{airflow_user}/airflow/airflow-worker.pid
Restart=on-failure
RestartSec=5s
PrivateTmp=False

[Install]
WantedBy=multi-user.target

""")

	with open("/tmp/airflow-worker.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(('mv', '/tmp/airflow-worker.service', '/etc/systemd/system/airflow-worker.service'),
	    sudo=True
	)

	Execute(('systemctl', 'daemon-reload'),
	    sudo=True
	)


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

	for section, value in airflow_config.items():
		airflow_config_file += format("[{section}]\n{value}\n")

	with open("/tmp/airflow.cfg", 'w') as configFile:
		configFile.write(airflow_config_file)
	configFile.close()
	Execute(('mv', '/tmp/airflow.cfg', params.airflow_home + '/airflow.cfg'),
	    sudo=True
	)
	Execute(('chown', format("{airflow_user}:{airflow_group}"), format("{airflow_home}/airflow.cfg")),
	    sudo=True
	)

