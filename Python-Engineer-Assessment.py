
import pandas as pd
import csv
import requests
from io import StringIO
import xml.etree.ElementTree as ET
import boto3
import zipfile

class Lambda:
    '''
    The Lambda Object's arguments are as follows:

    :param url: The url used to parse the xml and extract the initial download link with the file_type DLTINS.

    

    There are three methods:


    :method download_link: Requests the url to get the xml file -> Parses the Xml file to return the download_link.

    :zip_extraction method: Downloads the zip file -> Extracts the xml file from the zip file

    :method xml_to_csv: Parses an xml file and converts it to a csv file.
    
    '''
    
    
    def __init__(self, url = None) -> None:
        self.url = url
    def download_link(self):
        '''
        The required path is obtained by using the class's url.
        
        Creates a binary file called'registers.xml' and writes the path data to it.
        Parse the xml file, locate the necessary node, and return the download link.
        
        '''
        self.resp = requests.get(self.url)
        with open('datafile.xml', 'wb') as f:
            f.write(self.resp.content)
        self.tree = ET.parse('datafile.xml')
        self.root = self.tree.getroot()
        
        self.link = ''
        for item in self.root[1].iter("doc"):
            if item.find("str[@name = 'file_type']").text == 'DLTINS':
                self.link = item.find("str[@name='download_link']").text
                break
        return self.link

    
    def zip_extraction(self, link = None):
        
        '''
        :param link: URL for downloading the zip file

        The link is used to request the link.

        Make a file called 'zip_file.zip' and put the content inside it.

        Extraction of the zip file, saving the file name from the namelist and returning it
        
        '''
        self.zip_file = requests.get(self.link)
        with open('data_zip_file.zip', 'wb') as f:
            f.write(self.zip_file.content)
        self.xml_file = ''
        with zipfile.ZipFile('data_zip_file.zip', 'r') as f:
            self.xml_file = f.namelist()[0]
            f.extractall('')
        return self.xml_file

    def xml_to_csv(self, xml = None):
        '''
        :param xml: xml file to be converted to csv
        
        Parse the xml file to discover the relevant tags using the following headers: 
        FinInstrmGnlAttrbts.Id,
         FinInstrmGnlAttrbts.FinInstrmGnlAttrbts,
          FullNm.FinInstrmGnlAttrbts, 
          ClssfctnTp.CmmdtyDerivInd,
           FinInstrmGnlAttrbts.NtnlCcy
            and Issr
        
        Returns a DataFrame with the above headers.
        
        '''
        
        self.new = ET.parse(xml)     #parse xml
        self.test = self.new.getroot()

        self.pattern = 'FinInstrmGnlAttrbts'     #required node
        self.children = ['Id', 'FullNm', 'ClssfctnTp', 'CmmdtyDerivInd', 'NtnlCcy']     #required children nodes
        
        self.tag = 'Issr' #required node

        self.rows  = []
        self.cols = [self.pattern + '.' + k for k in self.children]
        self.cols.append(self.tag)
        
        self.parent = 'TermntdRcrd'        #parent node
        
        for i in self.test.iter():         
            if self.parent in i.tag:       # If parent is found
                self.entry = [None for x in range(len(self.cols))]     # Create an array with the required elements.
                for child in i:
                    if self.pattern in child.tag:    # If required child has been found
                            for c in child:     # Get the required grand-children
                                for k in range(len(self.children)):
                                        if self.children[k] in c.tag:    # If grandchildren found, update entry
                                            self.entry[k] = c.text
                    if self.tag in child.tag:     # If Issr found
                        self.entry[5] = child.text
                self.rows.append(self.entry)      # Add to list of rows
                
                
        self.df = pd.DataFrame(self.rows, columns=self.cols)      
        return self.df

if __name__ == '__main__':
    
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100" #Requirement-1: save the download link to url and download the xml file
    p = Lambda(url) #create an object for class lambda 
    
    #Requirement 2: Please parse through the xml until you reach the first download link with the file_type DLTINS and download the zip.
    zip_link = p.download_link()
    
    #Requirement 3: Extract the xml file from the zip file.
    xml_file = p.zip_extraction(zip_link)

    #Requirement 4: Convert the xml's contents to a CSV.
    df = p.xml_to_csv(xml_file)
    df.to_csv('final_output.csv')

    #Requirement 5: Place the csv file from step 4 in an AWS S3 bucket.
    s3 = boto3.client("s3", aws_access_key_id = "AKIAY2ZMTJDKRSSDGR6D", aws_secret_access_key="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")
    csv_buf = StringIO()
    df.to_csv(csv_buf, header = True, index = False)
    csv_buf.seek(0)
    s3.put_object(Bucket="steeleye-assignment1", Body=csv_buf.getvalue(), Key='final_output.csv')
