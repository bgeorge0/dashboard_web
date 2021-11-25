@ECHO OFF
ECHO Script started >> update_dashboard.log
for /F "usebackq tokens=1,2 delims==" %%i in (`wmic os get LocalDateTime /VALUE 2^>NUL`) do if '.%%i.'=='.LocalDateTime.' set ldt=%%j
set ldt=%ldt:~0,4%-%ldt:~4,2%-%ldt:~6,2% %ldt:~8,2%:%ldt:~10,2%:%ldt:~12,6%
echo Local date is [%ldt%] >> update_dashboard.log

:: Check for Python instllation
python --version 2>NUL
if errorlevel 1 goto errorNoPython

:: If we get here, Python is installed
ECHO About to run count_delivery_records.py >> update_dashboard.log
Python.exe .\py\count_delivery_records.py
ECHO About to load_new_data.py >> update_dashboard.log
Python.exe .\py\load_new_data.py
ECHO FINISHED >> update_dashboard.log
goto :eof

: errorNoPython
echo.
echo Error^: Python not installed
PAUSE