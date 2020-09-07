# Name   : myfx_commoutlook_data_collecter.py
# Author : sanuja-gayantha (github.com) 
# Date   : 2020/09/07


import requests
import pandas as pd
import datetime 
import os
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import smtplib
from email.message import EmailMessage


#notes---------
#before run this enter your e-mail address and password to certen variables
#Allow less secure apps access ON mode (link - https://myaccount.google.com/lesssecureapps)


class Koyfin:

    date_to = ""
    date_from = ""
    clist = ['TIOC', 'XAL1', 'DA', 'CCX.CA', 'T1', 'CL1']


    def __init__(self, economic_event, koyfin_code, period, latest_economic_data_relase_date):

        self.economic_event = economic_event
        self.koyfin_code = koyfin_code
        self.period = period
        self.latest_economic_data_relase_date = latest_economic_data_relase_date

    #get latest date from server---------------------------------------------------------------------------------------------------------------------------------------------------
    def get_latest_economic_data_relase_date(self):

        #final link
        if self.koyfin_code in self.clist:
            link = ('https://api.koyfin.com/api/v2/commands/g/g.fut/' + self.koyfin_code + '?dateFrom=' + self.date_from + '&dateTo=' + self.date_to + '&period=' + self.period)
        else:
            link = ('https://api.koyfin.com/api/v2/commands/g/g.gec/' + self.koyfin_code + '?dateFrom=' + self.date_from + '&dateTo=' + self.date_to + '&period=' + self.period)

        response = requests.get(link)
        results = response.json();
        #print(results)
        self.latest_economic_data_relase_date = results['endDate'].split('T')[0]

        return self.latest_economic_data_relase_date


    #create paths to csv files and compaire latest data date with csv file date to check economic event update or not --------------------------------------------------------------
    def check_save_koyfin_data(self):

        current_path = os.getcwd()
        folder_full_path = os.path.join(current_path, self.economic_event)

        #create a folder if it not exists
        if not os.path.exists(folder_full_path):
            os.makedirs(folder_full_path)


        #path to file
        file_name = os.path.join(folder_full_path, f'{self.koyfin_code}.csv')

        # if file does not exist write header 
        if not os.path.isfile(file_name):
            with open(file_name , 'a') as myfile:
                myfile.write(self.latest_economic_data_relase_date + '\n')

                return self.latest_economic_data_relase_date, self.economic_event, self.koyfin_code


        else:
            data = pd.read_csv(file_name, header=None)
            #print(data)
            #print(list(data[0])[-1])

            #check equality of csv file date and server send date(web pade current date)
            if self.latest_economic_data_relase_date  != list(data[0])[-1]:
                #print("new update")
                data[[0]] = self.latest_economic_data_relase_date
                data.to_csv(file_name, header=None, index=None, sep=',', mode='a')

                return self.latest_economic_data_relase_date, self.economic_event, self.koyfin_code


    #create range of data requests----------------------------------------------------------------------------------------------------------------------------------------------------
    @classmethod
    def create_dates(cls):
        #create starting and ending parameters
        cls.current_date = datetime.date.today()
        cls.date_to = (cls.current_date + relativedelta(years=1)).strftime('%Y-%m-%d')
        cls.date_from = (cls.current_date - relativedelta(years=2)).strftime('%Y-%m-%d')







def check_dates():

    try:
        #This list contain all the economic events need to be update
        update_data = []

        #koyfn----------------------------------------------------------------------------------
        koyfin_codes = {
                        'Government_Debt_to_GDP_Ratio':['USADebt2GDP', 'EMUDebt2GDP', 'JPNDebt2GDP', 'GBRDebt2GDP', 'CHEDebt2GDP', 'SWEDebt2GDP', 'AUSDebt2GDP', 'NZLDebt2GDP', 'CANDebt2GDP', 'NORDebt2GDP'],
                        'Government_SD_GDP':['FDDSGDP', 'WDEBEURO', 'EHBBJPY', 'WCSDGBR', 'WCSDCHE', 'WCSDSWE', 'EHBBAR', 'WCSDNZL', 'CATGBDSD', 'WCSDNOR'], 
                        'ISM_PMI':['NAPMPMI', 'EUROAREAMANPMI', 'JAPANMANPMI', 'UNITEDKINMANPMI', 'SWITZERLANMANPMI', 'SWEDENMANPMI', 'AUSTRALIAMANPMI', 'NEWZEALANMANPMI', 'CANADAMANPMI', 'NORWAYMANPMI'],
                        'Service_PMI':['UNITEDSTANONMANPMI', 'EUROAREASERPMI', 'JAPANSERPMI', 'UNITEDKINSERPMI', 'SWITZERLANSERPMI', 'SWEDENSERPMI', 'AUSTRALIASERPMI', 'NEWZEALANSERPMI'],
                        'CSI':['UMCSENT', 'EUCCEMU', 'JCOMACF', 'UKCCI', 'SZCCI', 'SWECCI', 'WMCCCONPCT', 'NZCC', 'OECAI001', 'NOCONF'],
                        'Building_permits':['UNITEDSTABUIPER', 'SWEDENBUIPER', 'AUSTRALIABUIPER', 'NEWZEALANBUIPER', 'CANADABUIPER', 'NORWAYBUIPER'],            
                        'NFP':['NFP_TCH', 'EUROAREAEMPCHA', 'JAPANEMPPER', 'UNITEDKINEMPCHA', 'SWITZERLANNONFARPAY', 'SWEDENEMPCHA', 'AUSTRALIAEMPCHA', 'NEWZEALANEMPCHA', 'CANADANONFARPAY', 'NORWAYPROPRI'],
                        'M2_money_supply':['UNITEDSTAMONSUPM2', 'EMUEVOLVMONSUPM2', 'JAPANMONSUPM2', 'UNITEDKINMONSUPM2', 'SWITZERLANMONSUPM2', 'SWEDENMONSUPM2', 'CANADAMONSUPM2', 'NORWAYMONSUPM2'],
                        'Interest_rate':['FDTR', 'EURR002W', 'BOJDTR', 'UKBRBASE', 'SZLTTR', 'SWRRATEI', 'RBATCTR', 'NZOCRS', 'CCLR', 'NOBRDEP'],
                        'CPI':['UNITEDSTACONPRIINDCP', 'EUROAREACONPRIINDCP', 'JAPANCONPRIINDCPI', 'UNITEDKINCONPRIINDCP', 'SWITZERLANCONPRIINDC', 'SWEDENCONPRIINDCPI', 'AUSTRALIACONPRIINDCP', 'NEWZEALANCONPRIINDCP', 'CANADACONPRIINDCPI', 'NORWAYCONPRIINDCPI'],
                        'Core_CPI':['UNITEDSTACORCONPRI', 'EUROAREACORCONPRI', 'JAPANCORCONPRI', 'UNITEDKINCORCONPRI', 'SWITZERLANCORCONPRI', 'SWEDENCORCONPRI', 'AUSTRALIACORCONPRI', 'NEWZEALANCORCONPRI', 'CANADACORCONPRI', 'NORWAYCORCONPRI'],
                        'PPI':['UNITEDSTAPROPRI', 'EUROAREAPROPRI', 'JAPANPROPRI', 'UNITEDKINPROPRI', 'SWITZERLANPROPRI', 'SWEDENPROPRI', 'AUSTRALIAPROPRI', 'NEWZEALANPROPRI', 'CANADAPROPRI', 'NORWAYPROPRI'],
                        'Central_bank_balance_sheet':['UNITEDSTACENBANBALSH', 'EUROAREACENBANBALSHE', 'JAPANCENBANBALSHE', 'UNITEDKINCENBANBALSH', 'SWITZERLANCENBANBALS', 'SWEDENMONSUPM2', 'AUSTRALIACENBANBALSH', 'NEWZEALANCENBANBALSH', 'CANADACENBANBALSHE', 'NORWAYCENBANBALSHE'],
                        'Commodity':['PCOALAUUSDM', 'TIOC', 'XAL1', 'CHIMCOALA', 'JPIMZAGTA', 'DA', 'CCX.CA', 'T1', 'CL1', 'NORWAYCRUOILPRO', 'NORWAYCRUOILRIG'],
                        'GDP_Growth_Rate_quartely':['GDP_CQOQ', 'EUGNEMUQ', 'JGDPAGDP', 'UKGRYBZQ', 'SZGDPCQQ', 'SWGDPAQQ', 'AUNAGDPC', 'NZNTGDPC', 'CGE9QOQ', 'NOGDCOSQ']
                       }

        period = ['monthly']

        economic_events = ['Government_Debt_to_GDP_Ratio', 'Government_SD_GDP', 'ISM_PMI', 'Service_PMI', 'CSI', 'Building_permits', 'NFP', 'M2_money_supply',  'Interest_rate', 
                           'CPI', 'Core_CPI', 'PPI', 'Central_bank_balance_sheet', 'Commodity', 'GDP_Growth_Rate_quartely']

        numberOfElements = 0

        #iterate dictonary data
        for count1,economic_event in enumerate(economic_events):

            for count2,code in enumerate(koyfin_codes[economic_event]):

                koyfin_code = code
                instanse_name = 'em_' + str(count1) + str(count2)
                latest_economic_data_relase_date = ""

                instance = Koyfin(economic_event, code, period[0], latest_economic_data_relase_date)
                Koyfin.create_dates()
                latest_date = instance.get_latest_economic_data_relase_date()
                return_data = instance.check_save_koyfin_data()

                #appen to list/ data ready to send
                if return_data:
                    update_data.append(return_data)

                print(instanse_name, economic_event, code, latest_date)
                numberOfElements += 1

        



        #Euro area_10_year_Government_bond_yield code--------------------------------------------------
        link = 'https://sdw.ecb.europa.eu/browseTable.do?org.apache.struts.taglib.html.TOKEN=3c27c867d2c7f38610c4d60e08874491&df=true&ec=&dc=&oc=&pb=&rc=&DATASET=0&removeItem=&removedItemList=&mergeFilter=&activeTab=FM&showHide=&MAX_DOWNLOAD_SERIES=500&SERIES_MAX_NUM=50&node=qview&SERIES_KEY=143.FM.M.U2.EUR.4F.BB.U2_10Y.YLD'
        source = requests.get(link)
        soup = BeautifulSoup(source.text, 'lxml')

        content = soup.find('table', id='dataTableID', class_='tablestats')
        for count,i in enumerate(content.find_all('tr')):
            if count == 3:
                Euro_area_10_year_Government_bond_yield_relase_date = list(i)[0].text
                break


        #create instance using Koyfin class to check equality
        economic_event = 'Euro_area_10_year_Government_bond_yield'
        code = 'Euro'
        latest_economic_data_relase_date = str(Euro_area_10_year_Government_bond_yield_relase_date)

        EGBYield = Koyfin(economic_event, code, period[0], latest_economic_data_relase_date)
        return_data = EGBYield.check_save_koyfin_data()

        if return_data:
            update_data.append(return_data)

        numberOfElements += 1
        print('Number of elements = ', numberOfElements)


        return update_data

    except Exception as e:
        #Save all execeptions in txt file   
        with open("Exceptions.txt", 'a') as ex:
            ex.write(str(e) + '\n')
        pass


def send_email(EMAIL_ADDRESS, EMAIL_PASSWORD):

    try:
        update_data = check_dates()
        #print(update_data)

        #checking is there are any data in update_list if  data exists condition will True
        if update_data:

            EMAIL_ADDRESS = EMAIL_ADDRESS
            EMAIL_PASSWORD = EMAIL_PASSWORD

            msg = EmailMessage()
            msg['Subject'] = 'New update'
            msg['From'] = EMAIL_ADDRESS
            msg['To'] = EMAIL_ADDRESS
            msg.set_content('This is a plain text e mail')

            #HTML parts of code------------------------------------------------------------------------
            #starting part of html code
            html_code_first = ("""\

                <!DOCTYPE html>
                <html lang="en">

                <head>
                  <h4>
                    <i><u>New Economic data receved for those events</u></i>
                  </h4>
                </head>

                <body>
                  <table>
                    <tr>
                      <th><i><u>Data relase date</u></i></th>
                      <th>&nbsp; &nbsp; &nbsp; &nbsp; <i><u>Economic event</u></i></th>
                      <th> &nbsp; &nbsp; &nbsp; &nbsp; <i><u>Code</u></i></th>
                    </tr>

                """)

            #iterate update_data list for add values to html code
            for line in update_data:

                Data_relase_date = line[0]
                Economic_event = line[1]
                Code = line[2]

                add_html = f'<tr align="left"> <td>{Data_relase_date}</td><td>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;{Economic_event}</td><td>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;{Code}</td></tr>' 
                html_code_first += add_html

            #ending part of html code
            html_code_last = '</table></body></html>'

            #full html code
            full_html_code = html_code_first + html_code_last
            

            #HTML code pass
            msg.add_alternative(full_html_code, subtype='html')

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:

                smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
                smtp.send_message(msg)


            print("Email sent successfully...")    

    except Exception as e:
        #Save all execeptions in txt file   
        with open("Exceptions.txt", 'a') as ex:
            ex.write(str(e) + '\n')
        pass
        


if __name__ == '__main__':

	EMAIL_ADDRESS = 'enter your g_mail address'
    EMAIL_PASSWORD = 'password'
    send_email(EMAIL_ADDRESS, EMAIL_PASSWORD)
    



