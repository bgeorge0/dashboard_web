# Import the following library to make use of the DispatchEx to run the macro
import logging
import os
import win32com.client as wincl
import datetime
import shutil

''' function to run macro '''
def runMacro(pth):
    logging.info('Opening : ' + pth)
    if os.path.exists(pth):
        # DispatchEx is required in the newest versions of Python.
        excel_macro = wincl.DispatchEx("Excel.application")
        workbook = excel_macro.Workbooks.Open(Filename = pth, ReadOnly =1)
        logging.info('Workbook open, running macro')
        excel_macro.Application.Run("ThisWorkbook.ImportAll")
        logging.info('Macro run')

        try:
            excel_macro.Application.Quit()
            logging.info('Workbook quit')
        except e:
            logging.info('Cannot Quit - workbook probably open')
            logging.info(e)
        finally:
            del excel_macro
            logging.info('Deleted var')

# Start logging
logging.basicConfig(filename='update_dashboard.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

# Set now time
now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
logging.info('STARTING AT : ' + now_time)

# Run macro
path = r"C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\xlsm_dashboard\src\xlsm\MRL_dashboard.xlsm"
runMacro(path)