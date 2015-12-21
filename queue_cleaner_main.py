##################################################################################
# Author: Justin Stubbs (u350932)
# Create date: 08/07/2015
# Description: Runs locally on the ctm server and deletes jobs in MIDRANGE.
# Part of Queue Cleaner automation.
# Requires Python 2.7.x
# Usage: python del_jobs_main.py <date> <odate>
#       <date> -> yyyymmdd Is the date equal to or older than you want
#                           to delete jobs from (do not use current date)
#        <odate> -> current odate
##################################################################################
 
import sys
import subprocess
import shlex
from datetime import datetime
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import io
 
DB_PASSWORD = 'p'
DATABASE_NAME="u"
DATABASE_USER="u"
DATABASE_PORT="0000"
SUP_EMAIL_TO = 's@x.com.au'
SUP_EMAIL_FROM = 's@x.com.au'
SMTP_SERVER = 'z'
 
def check_user_args(user_args):
    if time.strftime("%Y%m%d") == user_args[1]:
        print 'You cant delete all jobs from the current date! Change your <date_to_delete_from> parameter.'
        sys.exit(1)
    try:
        date_string, odate = sys.argv[1:] # check they only enter 2 arguments
    except ValueError:
        sys.exit('Usage: queue_cleaner <date_to_delete_from> <odate>')
    try:
        input_date = datetime.strptime(date_string, '%Y%m%d').date() # check date format ok
    except ValueError:
        sys.exit('expected date in YYYYMMDD format, got: {}'.format(date_string))
    try:
        datetime.strptime(odate, '%y%m%d').date() # check ODATE format ok
    except ValueError:
        sys.exit('expected ODATE in YYMMDD format, got: {}'.format(date_string))
    return date_string, odate
 
def list_old_jobs_to_hold(delete_date, odate, env_variables):
    # some jobs may already be held or deleted. The utility cannot tell us this so we use a db query to filter
    database_query="select order_id from a" + odate + "002_ajob where order_time < " \
                   + "'" + delete_date + "'" \
                   + " and not (status = 'Ended OK' or state LIKE 'Deleted %' or state LIKE 'Held %')" + ";"
    #print database_query
    list_jobs = do_sh_shell_command('psql -d ' + DATABASE_NAME +  ' -U ' + DATABASE_USER + ' -p '
                        + DATABASE_PORT + ' -c ' + '"' + database_query + '"'
                        , env_variables)
    return list_jobs[1].split('\n')
 
def list_old_jobs_to_delete(delete_date, odate, env_variables):
    # filter on all jobs older than date x that have not been deleted
    database_query="select order_id from a" + odate + "002_ajob where order_time < " \
                   + "'" + delete_date + "'" + " and not (state LIKE 'Deleted %') and state LIKE 'Held %'" + ";"
    #print database_query
    list_jobs = do_sh_shell_command('psql -d ' + DATABASE_NAME +  ' -U ' + DATABASE_USER + ' -p '
                        + DATABASE_PORT + ' -c ' + '"' + database_query + '"'
                        , env_variables)
    return list_jobs[1].split('\n')
 
def do_sh_shell_command(string_command, env_variables=None):
    cmd = shlex.split(string_command)
    try:
       p = subprocess.check_output(string_command, shell=True,
                                   env=env_variables) # shell=True means sh shell used
    except subprocess.CalledProcessError as e:
        print e.output
        print 'Error running command: ' + '"' + e.cmd + '"' + ' return code: ' + str(e.returncode) + ', see above shell error'
        return e.returncode, e.cmd
    return 0, p
 
def hold_ajf_job(job_order_id):
    return do_sh_shell_command('ctmpsm -UPDATEAJF ' + job_order_id + ' HOLD')
 
def delete_ajf_job(job_order_id):
    return do_sh_shell_command('ctmpsm -UPDATEAJF ' + job_order_id + ' DELETE')
 
def email_undeletable_jobs(list_jobs, email):
    attached_file = ",".join(list_jobs)
    msg = MIMEMultipart()
    msg['Subject'] = 'PCTMON5D: Automation unable to delete MIDRANGE_TEST jobs'
    msg['From'] = SUP_EMAIL_FROM
    msg['To'] = SUP_EMAIL_TO
    email_body_message = \
    '<html>' \
    '<body>' \
    '<p>' \
    "The attached job order_id's were not able to be deleted via automation." \
    '<br>' \
    '<br>' \
    ' Please follow ' \
    '<a href="http://google.com">this</a>' \
    ' guide to delete them from the ajf.' \
    '</p>' \
    '</body>' \
    '</html>'
    body = MIMEText(email_body_message, 'html')
    msg.attach(body)
    f = io.StringIO(unicode(attached_file))
    attachment = MIMEText(f.getvalue())
    f.close()
    attachment.add_header('Content-Disposition', 'attachment',
                          filename='undeletable job order_ids MIDRANGE_TEST.txt')          
    msg.attach(attachment) 
    s = smtplib.SMTP(SMTP_SERVER)
    s.sendmail(msg['From'], msg['To'], msg.as_string())
    s.quit()
   
def main():
    user_arguments = check_user_args(sys.argv)
    my_env = os.environ # get current env variables
    my_env['PGPASSWORD'] = DB_PASSWORD # add pgppassword to enviroment variables
                                       # so not prompted for pw doing psql commands
    jobs_to_hold = list_old_jobs_to_hold(user_arguments[0], user_arguments[1], my_env)
    for j in range(2, len(jobs_to_hold)-3): # format the list getting rid of anything
                                            # that is not an order id
        held_job = hold_ajf_job(jobs_to_hold[j].strip())
        if held_job[0] == 0:
            print held_job[1]
    jobs_unable_to_delete = [] # create an empty list to keep track of undeletable jobs
    jobs_to_delete = list_old_jobs_to_delete(user_arguments[0], user_arguments[1], my_env)
    for j in range(2, len(jobs_to_delete)-3): # format the list getting rid of anything
                                              # that is not an order id
        delted_job = delete_ajf_job(jobs_to_delete[j].strip())
        if delted_job[0] != 0:
            jobs_unable_to_delete.append(jobs_to_delete[j].strip())
        else:
            # note the utility adds extra 000 to the start of the order id, you canot filter on these
            # eg order 0000fjrf should be filtered on as 0fjrf -> remove leading 000
            print delted_job[1]
    if len(jobs_unable_to_delete) > 0:
        for j in jobs_unable_to_delete:
            print 'unable to delete job order_id: ' + j
        email_undeletable_jobs(jobs_unable_to_delete, SUP_EMAIL_TO)
 
if __name__ == "__main__":
    sys.exit(main())
