set url=tcp://localhost

cd PuppetMaster\bin\Debug
start PuppetMaster.exe --Dgui
::start PuppetMaster.exe
::start PuppetMaster.exe

cd ..\..\..\Client\bin\Debug
::start Client.exe %url%:55001/W1