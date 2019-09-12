import sys, os, pwd, grp, signal, time
from resource_management import *
from resource_management.core import sudo
from resource_management.core.shell import as_sudo
from subprocess import call
from airflow_setup import *

class AirflowScheduler(Script):
	"""
	Contains the interface definitions for methods like install,
	start, stop, status, etc. for the Airflow Server
	"""
	def install(self, env):
		import params
		env.set_params(params)
		self.install_packages(env)
		Logger.info('Installing Airflow-scheduler Service')

		Execute(('useradd', '-m', format("{airflow_user}")),
		    ignore_failures=True,
		    sudo=True
		)

		# Create virtualenv
		Execute('virtualenv -p python3 --clear ~/venv-airflow',
			user=params.airflow_user)

		# Install dependencies
		Execute(format("source ~/venv-airflow/bin/activate && pip install --upgrade {airflow_pip_params} pip wheel setuptools"),
			user=params.airflow_user)
		Execute(format("source ~/venv-airflow/bin/activate && pip install {airflow_pip_params} secure-smtplib"),
			user=params.airflow_user)

		# Install Airflow
		Execute(format("source ~/venv-airflow/bin/activate && pip install {airflow_pip_params} apache-airflow[all]==1.10.5"),
			user=params.airflow_user)

		# Add syslog and apply
		File("/etc/rsyslog.d/airflow-scheduler.conf",
		    mode=0644,
		    owner=params.airflow_user,
		    group=params.airflow_group,
		    content=format("""
if $programname  == 'airflow-scheduler' then {airflow_log_dir}/scheduler.log
& stop
		    """)
		)
		Execute(('systemctl', 'restart', 'rsyslog'),
		    sudo=True)

		# Add logrotate and apply
		File("/etc/logrotate.d/airflow",
		    mode=0644,
		    owner=params.airflow_user,
		    group=params.airflow_group,
		    content=format("""
{airflow_log_dir}/*.log
{{
    missingok
    daily
    copytruncate
    rotate 7
    notifempty
}}
		    """)
		)

		# Initialize Airflow database
		Execute('source ~/venv-airflow/bin/activate && airflow initdb',
			user=params.airflow_user)


		Logger.info('Setting up Rabbitmq-server')

		# Enable rabbitmq management plugin
		Execute(('rabbitmq-plugins', 'enable', 'rabbitmq_management'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})

		# Start rabbitmq
		Execute(('systemctl', 'start', 'rabbitmq-server'),
		    sudo=True)

		# Return rabbitmq' virginity
		Execute(('rabbitmqctl', 'stop_app'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('rabbitmqctl', 'force_reset'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('rabbitmqctl', 'start_app'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})

		# Add Airflow user as administrator
		Execute(('rabbitmqctl', 'add_user', params.airflow_user, 'runrabbitrun'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('rabbitmqctl', 'add_vhost', 'airflow'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('rabbitmqctl', 'set_user_tags', params.airflow_user, 'administrator'),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('rabbitmqctl', 'set_permissions', '-p', '/', params.airflow_user, "\".*\"", "\".*\"", "\".*\""),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('rabbitmqctl', 'set_permissions', '-p', 'airflow', params.airflow_user, "\".*\"", "\".*\"", "\".*\""),
		    sudo=True,
		    environment={'HOME': params.airflow_home})
		Execute(('systemctl', 'stop', 'rabbitmq-server'),
		    sudo=True)

	def configure(self, env):
		import params
		env.set_params(params)
		airflow_configure(env)
		airflow_make_systemd_scripts_scheduler(env)

	def start(self, env):
		import params
		self.configure(env)
		Execute(('systemctl', 'enable', 'rabbitmq-server'),
		    sudo=True)
		Execute(('systemctl', 'start', 'rabbitmq-server'),
		    sudo=True)
		Execute(('systemctl', 'enable', 'airflow-scheduler'),
		    sudo=True)
		Execute(('systemctl', 'start', 'airflow-scheduler'),
		    sudo=True)

	def stop(self, env):
		import params
		env.set_params(params)
		Execute(('systemctl', 'stop', 'airflow-scheduler'),
		    sudo=True)
		Execute(('systemctl', 'disable', 'airflow-scheduler'),
		    sudo=True)
		File(params.airflow_scheduler_pid_file,
			action = "delete",
			owner = params.airflow_user)
		Execute(('systemctl', 'stop', 'rabbitmq-server'),
		    sudo=True)
		Execute(('systemctl', 'disable', 'rabbitmq-server'),
		    sudo=True)


	def status(self, env):
		import status_params
		env.set_params(status_params)
		#use built-in method to check status using pidfile
		check_process_status(status_params.airflow_scheduler_pid_file)

if __name__ == "__main__":
	AirflowScheduler().execute()
