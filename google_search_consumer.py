import configparser
import os
import json
import re
import time
import traceback

import dateparser as dateparser
import psycopg2
from psycopg2 import extras
import boto3
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import requests

# todo: add a new field to google_search_result to differentiate search results in the carousel.


def main():
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    c = conn.cursor(cursor_factory=extras.RealDictCursor)

    google_subquery = None
    incomplete_transaction = False
    ip = None

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    c.execute("select * from aws_credentials")
    aws_credential = c.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')
    google_queue = sqs.get_queue_by_name(QueueName='google_search')
    # Retrieve AWS AMI.
    c.execute("select * from aws_ami "
              "where ami_name = 'google_search_consumer' "
              "and region_name = '{}'".format(aws_credential['region_name']))
    aws_ami = c.fetchone()

    try:
        ip = requests.get('http://checkip.amazonaws.com').text.rstrip()

        # if it is running in the cloud, switch to headless mode
        options = Options()
        if 'DISPLAY' not in os.environ:
            options.headless = True
        # start the browser
        with webdriver.Firefox(firefox_options=options) as driver:
            blocked = False
            google_queue_empty = False
            while not google_queue_empty and not blocked:
                message = google_queue.receive_messages(MaxNumberOfMessages=1)
                # if queue is empty
                if len(message) == 0:
                    google_queue_empty = True
                else:
                    google_subquery = json.loads(message[0].body)
                    message[0].delete()
                    incomplete_transaction = True
                    driver.get(google_subquery['query_url'])
                    input_google = driver.find_elements_by_id("logo")
                    # if you cannot see Google's logo, you have been blocked
                    if len(input_google) == 0:
                        blocked = True
                    else:
                        results = list()
                        there_is_next = True
                        while there_is_next and not blocked:
                            if len(driver.find_elements_by_css_selector('td.cur')) > 0:
                                current_page = int(driver.find_element_by_css_selector('td.cur').text)
                            else:
                                current_page = None

                            results_current_page = driver.find_elements_by_css_selector('div.srg div.rc')
                            for result in results_current_page:
                                new_result = dict()
                                new_result['query_alias'] = google_subquery['query_alias']
                                new_result['query_date'] = google_subquery['query_date']
                                new_result['page_number'] = current_page
                                # initialize the fields that will be populated below
                                new_result['url'] = None
                                new_result['title'] = None
                                new_result['rank'] = None
                                new_result['date'] = None
                                new_result['blurb_text'] = None
                                new_result['blurb_html'] = None
                                new_result['missing'] = None

                                headline = result.find_elements_by_css_selector('h3.r a')
                                if len(headline) > 0:
                                    new_result['url'] = headline[0].get_attribute('href')
                                    new_result['title'] = headline[0].text
                                    if headline[0].get_attribute('onmousedown') is not None:
                                        rank = re.findall("(?:')([0-9]+)(?:')",
                                                          headline[0].get_attribute('onmousedown'))
                                        if len(rank) > 0:
                                            new_result['rank'] = int(rank[0])

                                result_date = result.find_elements_by_css_selector('span.f')
                                if len(result_date) > 0:
                                    if dateparser.parse(result_date[0].text[:-2]) is not None:
                                        new_result['date'] = dateparser.parse(result_date[0].text[:-2]).date()

                                blurb = result.find_elements_by_css_selector('span.st')
                                if len(blurb) > 0:
                                    new_result['blurb_text'] = blurb[0].text
                                    new_result['blurb_html'] = blurb[0].get_attribute('innerHTML')

                                missing = result.find_elements_by_css_selector('div._Tib')
                                if len(missing) > 0:
                                    new_result['missing'] = missing[0].text

                                results.append(new_result)

                            results_top_stories = driver.find_elements_by_tag_name('g-inner-card')
                            for result in results_top_stories:
                                new_result = dict()
                                new_result['query_alias'] = google_subquery['query_alias']
                                new_result['query_date'] = google_subquery['query_date']
                                new_result['page_number'] = current_page
                                # initialize the fields that will be populated below
                                new_result['url'] = None
                                new_result['title'] = None
                                new_result['rank'] = None
                                new_result['date'] = None
                                new_result['blurb_text'] = None
                                new_result['blurb_html'] = None
                                new_result['missing'] = None

                                headline = result.find_elements_by_tag_name('a')
                                if len(headline) > 0:
                                    new_result['url'] = headline[0].get_attribute('href')
                                    new_result['title'] = headline[0].text
                                    if headline[0].get_attribute('onmousedown') is not None:
                                        rank = re.findall("(?:')([0-9]+)(?:')",
                                                          headline[0].get_attribute('onmousedown'))
                                        if len(rank) > 0:
                                            new_result['rank'] = int(rank[0])

                                result_date = result.find_elements_by_css_selector('span.f')
                                if len(result_date) > 0:
                                    new_result['date'] = dateparser.parse(result_date[0].text[:-2]).date()

                                results.append(new_result)

                            next_page = driver.find_elements_by_css_selector('a#pnnext.pn')
                            if len(next_page) == 0:
                                there_is_next = False
                            else:
                                next_page[0].click()
                                time.sleep(5)
                                input_google = driver.find_elements_by_id("logo")
                                if len(input_google) == 0:
                                    blocked = True
                        # if it was not blocked, add search results to database
                        if not blocked:
                            c.execute("""insert into google_search_attempt
                                         (query_alias, query_date, success, ip)
                                         values (%s, %s, %s, %s)""",
                                      (google_subquery['query_alias'], google_subquery['query_date'], True, ip))
                            if len(results) != 0:
                                data_text = ','.join(c.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                                                               (result['query_alias'],
                                                                result['query_date'],
                                                                result['page_number'],
                                                                result['url'],
                                                                result['title'],
                                                                result['rank'],
                                                                result['date'],
                                                                result['blurb_text'],
                                                                result['blurb_html'],
                                                                result['missing'],)).decode('utf-8')
                                                     for result in results)
                                c.execute("""insert into google_search_result
                                             (query_alias, query_date, page_number,
                                             url, title, rank, date, blurb_text, blurb_html, missing)
                                             values {}""".format(data_text))
                            conn.commit()
                            incomplete_transaction = False
            if blocked:
                # return URL to queue
                google_queue.send_message(MessageBody=json.dumps(google_subquery))
                incomplete_transaction = False
                # add record indicating google blocked requests
                c.execute("""insert into google_search_attempt
                             (query_alias, query_date, success, ip)
                             values (%s, %s, %s, %s)""",
                          (google_subquery['query_alias'], google_subquery['query_date'],
                           False, ip))
                conn.commit()
                # start a new server
                ec2 = aws_session.resource('ec2')
                # https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
                ec2.create_instances(ImageId=aws_ami['ami_id'],
                                     InstanceType='t2.nano',
                                     KeyName=aws_ami['key_pair_name'],
                                     InstanceInitiatedShutdownBehavior='terminate',
                                     MaxCount=1,
                                     MinCount=1)
    except Exception:
        conn.rollback()
        # add record indicating error.
        c.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                  (json.dumps(google_subquery), traceback.format_exc(), 'google_search_consumer', ip), )
        conn.commit()
        # return URL to queue
        if incomplete_transaction:
            google_queue.send_message(MessageBody=json.dumps(google_subquery))
        # start a new server
        ec2 = aws_session.resource('ec2')
        # https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
        ec2.create_instances(ImageId=aws_ami['ami_id'],
                             InstanceType='t2.nano',
                             KeyName=aws_ami['key_pair_name'],
                             InstanceInitiatedShutdownBehavior='terminate',
                             MaxCount=1,
                             MinCount=1)
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
