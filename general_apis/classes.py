# This file contains all the general classes used by the Automation Team 
# in the Automation Portal.

import pandas as pd
import requests
import openpyxl
import datetime
import pyodbc
import csv
import os
import re
from collections import Counter
import plotly.express as px
import plotly.io as pio
import glob
from sqlalchemy.engine import URL , create_engine
from django.conf import settings
from django.templatetags.static import static
import base64
from pathlib import Path
import logging
# Sending Mails Imports
from django.core.mail import send_mail, EmailMessage
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from general_apis.constants import *


class FileManipulation:
    """This class will be used to read/write all types of files.
    """
    
    def __init__(self,
                 file = None,
                 file_name = None,
                 app_name = None,
                 file_path_output = None,
                 file_path = None,
                 file_type = None,
                 file_contents = None):
        """This is the constructor "init" function which is called whenever an object of the class
        is initialized.

        Args:
            file (file object, optional): This parameter holds a file object. example: request.FILES['testFile']. Defaults to None.
            file_path (str, optional): The path of the file for reading and writing. Defaults to None.
            file_type (str, optional): txt, csv or xlsx. Defaults to None.
            file_contents (optional): Holds the content of the file while reading/writing. Defaults to None.
        """
        
        if file is None:
            self.file = None
        else:
            self.file = file

        if file_name is None:
            self.file_name = str()
        else:
            self.file_name = file_name

        if app_name is None:
            self.app_name = str()
        else:
            self.app_name = app_name

        if file_path_output is None:
            self.file_path_output = self.get_file_path()
        else:
            self.file_path_output = file_path_output
            
        if file_path is None:
            self.file_path = None
        else:
            self.file_path = file_path

        if file_type is None:
            self.file_type = "txt"
        else:
            self.file_type = file_type
            
        if file_contents is None:
            self.file_contents = str()
        else:
            self.file_contents = file_contents

    def get_file_path(self):

        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = self.file_name + time_stamp + '.txt'
        self.file_name = file_name
        file_path = os.path.join(settings.STATIC_ROOT, f'{self.app_name}/output/' + file_name)
        return file_path
    
            
    def read_file(self, return_list=False, decoding_needed=False, encoding_needed=False, sheet_name=False):
        """This function is used to read a file (txt, csv or xlsx)

        Args:
            return_list (bool, optional): If True, the function returns a list of lines, 
            if False the function returns a string of the content. Defaults to False.
            
            decoding_needed (bool, optional): If True, decoding is done on the content. 
            Used when taking a file as input form Django. Defaults to False.
        """
        
        # If the file type is txt
        if self.file_type == "txt":
            self.read_txt(return_list, decoding_needed, encoding_needed)
        # If the file type is csv
        elif self.file_type == "csv":
            self.read_txt(return_list, decoding_needed, encoding_needed)
        # If the file type is xlsx
        elif self.file_type == "xlsx":
            self.read_excel(sheet_name)
           
    def read_txt(self, return_list, decoding_needed, encoding_needed):
        """This function is used to read a txt or csv file.

        Args:
            return_list (bool, optional): If True, the function returns a list of lines, 
            if False the function returns a string of the content. Defaults to False.
            
            decoding_needed (bool, optional): If True, decoding is done on the content. 
            Used when taking a file as input form Django. Defaults to False.
        """
        
        if self.file == None and self.file_path != None:
            
            if encoding_needed:
                self.file = open(self.file_path, 'r', encoding="utf8")
            else:
                self.file = open(self.file_path, 'r')
            
        if self.file != None:
            # If the output is a list of decoded lines 
            if return_list and decoding_needed:
                lines = self.file.readlines()
                self.file_contents = []
                for line in lines:
                    # Decoding a string encoded in UTF-8 format
                    self.file_contents.append(line.decode('utf-8').replace("\r", ""))
            
            # If the output is a list of lines
            elif return_list:
                self.file_contents = self.file.readlines()
            
            # If the output is a decoded string
            elif decoding_needed:
                self.file_contents = self.file.read()
                try:
                    self.file_contents = self.file_contents.decode('utf-8').replace("\r", "")
                except:
                    raise Exception("This file does not need decoding. Assign decoding_needed = False")
            
            #If the output is a string
            else:
                self.file_contents = self.file.read()
        
        else:
            raise Exception("No file or file path were given. Please assign one of them and try again.")
                       
    def read_excel(self, sheet_name=False):
        """This function is used to read a xlsx file.
        """
        
        # Reading the excel file inputs as a DataFrame
        if self.file:
            if sheet_name:
                self.file_contents = pd.read_excel(self.file, engine='openpyxl', sheet_name=sheet_name)
            else:
                self.file_contents = pd.read_excel(self.file, engine='openpyxl')
        elif self.file_path:
            if sheet_name:
                self.file_contents = pd.read_excel(self.file_path, engine='openpyxl', sheet_name=sheet_name)
            else:
                self.file_contents = pd.read_excel(self.file_path, engine='openpyxl')
        else:
            Exception("There is no File given to read")

    def write_file(self, writing_type="w"):
        """This function is used to write a file.

        Args:
            writing_type (str, optional): used "w" for write and "a" for append. Defaults to "w".
        """
        
        # If file type is txt
        if self.file_type == "txt":
            self.write_txt(writing_type)
        elif self.file_type == "csv":
            self.write_csv()
        elif self.file_type == "xlsx" or self.file_type == "xlsm":
            self.write_excel()
        
        output_url = static(f'/{self.app_name}/output/' + self.file_name)
        return output_url
    
    def write_txt(self, writing_type):
        """This function is used to write a txt file.

        Args:
            writing_type (str, optional): used "w" for write and "a" for append.
        """
        
        if self.file_path_output:
            # If the file contents is a single string
            if isinstance(self.file_contents, str):
                with open(self.file_path_output, writing_type) as f:
                    f.write(self.file_contents)
                f.close()
            
            # If the file contents is a list of strings
            elif isinstance(self.file_contents, list):
                with open(self.file_path_output, writing_type) as f:
                    for line in self.file_contents:
                        f.write(line)
                f.close()

            # # If the file contents is a dataframe
            # if isinstance(self.file_contents, pd.DataFrame):
            #     with open(self.file_path, writing_type) as f:
            #         df_to_str = self.file_contents.to_string()
            #         f.write(df_to_str)
            #     f.close()
        else:
            raise Exception("A File path does not exist. Please add one to file_path")
        
    def write_excel(self):
        """This function is used to write a excel file.
        """
        wb = pd.ExcelWriter(self.file_path_output)
        self.file_contents.to_excel(wb)
        wb.save()

    def write_csv(self):
        """This function is used to write a excel file.
        """
        self.file_contents.to_csv(self.file_path_output)

class Connection:
    def __init__(self,
                 driver,
                 server,
                 dbName,
                 userName,
                 password,
                 conn=None):
        
        self.driver = str(driver)
        self.server = str(server)
        self.dbName = str(dbName)
        self.userName = str(userName)
        self.password = str(password)
        self.conn = conn
        self.log_file_path =settings.OUTPUT_DIR / "logs" /"sql_queries.log"

        # Set up logging
        self.logger = logging.getLogger('sql_queries_logger')
        self.logger.setLevel(logging.DEBUG)

        # Ensure that the handler is not already added
        if not self.logger.hasHandlers():
            self.handler = logging.FileHandler(self.log_file_path)
            self.handler.setLevel(logging.DEBUG)

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            self.handler.setFormatter(formatter)

            self.logger.addHandler(self.handler)

    def connect(self):
        """This function is used to connect to a server.
        """
        try:
            self.conn = pyodbc.connect('Driver=' + self.driver + ';'
                                    'Server=' + self.server + ';'
                                        'Database=' + self.dbName + ';'
                                        'UID=' + self.userName + ';'
                                        'PWD=' + self.password + ';')
            
            self.logger.info('##################################################')
            self.logger.info(f'A Connection to {self.dbName} is issued')


        except Exception as e:
            raise Exception("Connection Failed" + str(e))

    def query_select(self, query):
        """This function is used to use the SELECT query with the database.

        Args:
            query (str): A str that contains the query needed to be executed.

        Returns:
            df: A dataframe which contains the output of the query.
        """
        

        df = pd.read_sql_query(query, con = self.conn)
        self.logger.info(f'A Query to {self.dbName} DB is executed')
        self.logger.info("The Query is: " + query.replace('\n', '').replace('\r', '') + "")
        

        return df
    
    def query_insert(self, query):
        """This function is used to use the INSERT query with the database.

        Args:
            query (str): A str that contains the query needed to be executed.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
            self.logger.info(f'A Query to {self.dbName} DB is executed')
            self.logger.info("The Query is: " + query.replace('\n', '').replace('\r', '') + "")

        except:
            self.close()
            raise Exception(f"Their was a problem with the Query: {query}")

    def close(self):
        """This function closes the connection
        """
        try:
            if self.conn != None:
                if self.conn:
                    self.conn.close()
                    
                    self.logger.info(f'A Connection to {self.dbName} is closed')
                    self.logger.info('##################################################')
        except:
            print('Their was no Connection')


class Emails:
    """This Class is used for Email Manipulation
    """
    
    def __init__(self,
                 sender = None,
                 receivers = None,
                 cc = None,
                 subject = None,
                 mail_body_html = None,
                 html_file_path = None,
                 body_str = None):
        
        if sender != None:
            self.sender = sender
        else:
            self.sender = str()
            
        if receivers != None:
            self.receivers = receivers
        else:
            self.receivers = list()

        if cc != None:
            self.cc = cc
        else:
            self.cc = list()
            
        if subject != None:
            self.subject = subject
        else:
            self.subject = list()
            
        if mail_body_html != None:
            self.mail_body_html = mail_body_html
        else:
            self.mail_body_html = str()
            
        if html_file_path != None:
            self.html_file_path = html_file_path
        else:
            self.html_file_path = str()

        if body_str != None:
            self.body_str = body_str
        else:
            self.body_str = str()
            
        self.log_file_path = os.path.join(Path(__file__).resolve().parent.parent, 'logs/emails.log')
        # Set up logging
        self.logger = logging.getLogger('sending_emails_logger')
        self.logger.setLevel(logging.DEBUG)

        # Ensure that the handler is not already added
        if not self.logger.hasHandlers():
            self.handler = logging.FileHandler(self.log_file_path)
            self.handler.setLevel(logging.DEBUG)

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            self.handler.setFormatter(formatter)

            self.logger.addHandler(self.handler)
    
    def send_email(self):
        """Function used to send email.
        """
        if MACHINE != REMOTE_MACHINE:
            pass
        else:
            try:
                with open(self.html_file_path, 'w') as f:
                    f.write(self.mail_body_html)
            except:
                raise Exception("Couldn't write the HTML file")
                
            try:
                html_message = render_to_string(self.html_file_path)
                plain_message = strip_tags(html_message)
                send_mail(self.subject, plain_message, self.sender, self.receivers, html_message=html_message)
            except:
                raise Exception("Couldn't send the email")
            
            self.logger.info('##################################################')
            self.logger.info(f'An Email is sent to {self.receivers} with the body of {self.html_file_path}')
            
    def send_email_cc(self):
        """Function used to send email + cc.
        """
        if MACHINE != REMOTE_MACHINE:
            pass
        else:   
            try:
                email = EmailMessage(
                    subject = self.subject,
                    body = self.body_str,
                    from_email = self.sender,
                    to = self.receivers,
                    cc = self.cc
                )
                email.send()
            except:
                raise Exception("Couldn't send the email")
            
            self.logger.info('##################################################')
            self.logger.info(f'An Email is sent to {self.receivers} with the body of {self.html_file_path}')
     
 
from general_apis.constants import *







