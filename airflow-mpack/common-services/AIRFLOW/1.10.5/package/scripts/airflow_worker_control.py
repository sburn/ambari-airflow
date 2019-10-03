import sys, os, pwd, grp, signal, time
from resource_management import *
from resource_management.core import sudo
from resource_management.core.shell import as_sudo
from subprocess import call
from airflow_setup import *

class AirflowWorker(Script):
	"""
	Contains the interface definitions for methods like install, 
	start, stop, status, etc. for the Airflow Server
	"""
	def install(self, env):
		import params
		env.set_params(params)
		self.install_packages(env)
		Logger.info(format("Installing Airflow-worker Service"))

		# Add Airflow' group and user
                Execute(('groupadd', format("{airflow_group}")),
                    ignore_failures=True,
                    sudo=True)
                Execute(('useradd', '-m', '-g', format("{airflow_group}"), format("{airflow_user}")),
                    ignore_failures=True,
                    sudo=True)

		# Install Airflow dependencies
		Execute(('python3', '-m', 'pip', 'install', '--upgrade', format('{airflow_pip_params}'), 'pip', 'wheel', 'setuptools'),
		    sudo=True)
		Execute(('python3', '-m', 'pip', 'install', format('{airflow_pip_params}'), 'secure-smtplib'),
		    sudo=True)

		# Install Airflow
		Execute(('python3', '-m', 'pip', 'install', format('{airflow_pip_params}'), 'apache-airflow[all]==1.10.5'),
		    sudo=True)


                File("/etc/rsyslog.d/airflow-worker.conf",
                    mode=0644,
                    owner=params.airflow_user,
                    group=params.airflow_group,
                    content=format("""
if $programname  == 'airflow-worker' then {airflow_log_dir}/worker.log
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

                # Add sudoer for run_as_user in DAGs
                File("/etc/sudoers.d/airflow",
                    mode=0644,
                    owner='root',
                    group='root',
                    content=format("""
{airflow_user}         ALL=(%{airflow_group})      NOPASSWD: ALL
                    """)
                )

		# Initialize Airflow database
		Execute("/usr/local/bin/airflow initdb",
			user=params.airflow_user,
			environment={'AIRFLOW_HOME': params.airflow_home})

	def configure(self, env):
		import params
		env.set_params(params)
		airflow_configure(env)
		airflow_make_systemd_scripts_worker(env)
		
	def start(self, env):
		import params
		self.configure(env)
                Execute(('systemctl', 'enable', 'airflow-worker'),
                    sudo=True)
                Execute(('systemctl', 'start', 'airflow-worker'),
                    sudo=True)

	def stop(self, env):
		import params
		env.set_params(params)
                Execute(('systemctl', 'stop', 'airflow-worker'),
                    sudo=True)
                Execute(('systemctl', 'disable', 'airflow-worker'),
                    sudo=True)
		File(params.airflow_worker_pid_file,
			action = "delete")

	def status(self, env):
		import status_params
		env.set_params(status_params)
		#use built-in method to check status using pidfile
		check_process_status(status_params.airflow_worker_pid_file)

if __name__ == "__main__":
	AirflowWorker().execute()
