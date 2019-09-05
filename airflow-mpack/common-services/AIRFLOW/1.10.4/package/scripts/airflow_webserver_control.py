import sys, os, pwd, grp, signal, time
from resource_management import *
from resource_management.core import sudo
from resource_management.core.shell import as_sudo
from subprocess import call
from airflow_setup import *

class AirflowWebserver(Script):
	"""
	Contains the interface definitions for methods like install,
	start, stop, status, etc. for the Airflow Server
	"""
	def install(self, env):
		import params
		env.set_params(params)
		self.install_packages(env)
		Logger.info(format("Installing Airflow-webserver Service"))

		Execute(('useradd', '-m', format("{airflow_user}")),
                    ignore_failures=True,
                    sudo=True)

		# Create virtualenv
                Execute(format("virtualenv -p python3 --clear ~/venv-airflow"),
                        user=params.airflow_user)

		# Install dependencies
                Execute(format("source ~/venv-airflow/bin/activate && pip install --upgrade {airflow_pip_params} pip"),
                        user=params.airflow_user)
                Execute(format("source ~/venv-airflow/bin/activate && pip install {airflow_pip_params} wheel setuptools secure-smtplib psycopg2-binary"),
                        user=params.airflow_user)

		# Install Airflow
                Execute(format("source ~/venv-airflow/bin/activate && pip install {airflow_pip_params} 'apache-airflow[all_dbs,async,celery,cloudant,crypto,devel,devel_hadoop,druid,gcp,github_enterprise,google_auth,hdfs,hive,jdbc,kubernetes,ldap,mssql,mysql,oracle,password,postgres,qds,rabbitmq,redis,s3,samba,slack,ssh,vertica]==1.10.4'"),
                        user=params.airflow_user)

                File("/etc/rsyslog.d/airflow-webserver.conf",
                    mode=0644,
                    owner=params.airflow_user,
                    group=params.airflow_group,
                    content=format("""
if $programname  == 'airflow-webserver' then {airflow_log_dir}/webserver.log
& stop
		    """)
                )

                File("/etc/rsyslog.d/airflow-flower.conf",
                    mode=0644,
                    owner=params.airflow_user,
                    group=params.airflow_group,
                    content=format("""
if $programname  == 'airflow-flower' then {airflow_log_dir}/flower.log
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
		Execute(format("source ~/venv-airflow/bin/activate && airflow initdb"),
			user=params.airflow_user)

	def configure(self, env):
		import params
		env.set_params(params)
		airflow_configure(env)
		airflow_make_systemd_scripts_webserver(env)
		
	def start(self, env):
		import params
		self.configure(env)
		Execute(('systemctl', 'enable', 'airflow-webserver'),
		    sudo=True)
		Execute(('systemctl', 'start', 'airflow-webserver'),
		    sudo=True)
		Execute(('systemctl', 'enable', 'airflow-flower'),
		    sudo=True)
		Execute(('systemctl', 'start', 'airflow-flower'),
		    sudo=True)



	def stop(self, env):
		import params
		env.set_params(params)
		Execute(('systemctl', 'stop', 'airflow-webserver'),
		    sudo=True)
		Execute(('systemctl', 'disable', 'airflow-webserver'),
		    sudo=True)
		Execute(('systemctl', 'stop', 'airflow-flower'),
		    sudo=True)
		Execute(('systemctl', 'disable', 'airflow-flower'),
		    sudo=True)


		File(params.airflow_webserver_pid_file,
			action = "delete",
			owner = params.airflow_user)

	def status(self, env):
		import status_params
		env.set_params(status_params)
		#use built-in method to check status using pidfile
		check_process_status(status_params.airflow_webserver_pid_file)

	def initdb(self, env):
		import params
		env.set_params(params)
		self.configure(env)
		Execute(format("source ~/venv-airflow/bin/activate && airflow initdb"),
			user=params.airflow_user)

if __name__ == "__main__":
	AirflowWebserver().execute()
