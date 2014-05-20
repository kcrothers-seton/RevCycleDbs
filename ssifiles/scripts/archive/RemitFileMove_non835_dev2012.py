# remit file move

# next version:
    # line 50ish: should try to read file from .zip before removing.
    # function for compressing!

import os, string, datetime, time, glob, zipfile
from shutil import copy
from shutil import move
import win32com.client

extractpath = '\\\\AUSSSI\\SSIAPP\\RENWS\\BILLING\\UPLOADH'
exceptionpath = '\\\\AUSSSI\\SSIAPP\\RENWS\\BILLING\\UPLOADH\\FileArchive\\Exceptions'
financeloadpath = 'G:\\Finance\\Reimbursement\\Pass_through_reports'
logfilepath = 'N:\\PFS-DSS\\revcycledbs\\ssifiles\\logs'

dirfiletypelist = ['PLB', 'PYR','RP1']
fin_plb_payors = ['cad','car','chm']

logfile = os.path.join(logfilepath, 'log_' + datetime.datetime.today().strftime('%Y-%m-%d_%H%M') + '.TXT')
logfilehandle = open(logfile, 'w')
logfilehandle.write('Job started at ' + str(datetime.datetime.now().ctime()) + '.\n')

payernamelist = []

# script log file documentation as function
    # def (remitfile, job type, destination)
        # write(remitfile + ' has been '+ jobaction + ' to ' + loadpath + '.\n')

def archive(remitarchivepath,remityear,filetype,remitfile,remitfilename):
    try:
        zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' + '.zip'),'r')
    except:
        zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'w')
        zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
        logfilehandle.write(str(remitfilename) + ' has been compressed to ' + str(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip')) + '.\n')
        zfile.close()
    else:
        try:
            zfile.read(remitfile)
        except:
            zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'a')
            zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
            logfilehandle.write(str(remitfilename) + ' has been compressed to ' + str(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip')) + '.\n')
            zfile.close()

def movefiles():
    # parse extract file name data
    for filetype in dirfiletypelist:
        remitarchivepath = os.path.join(extractpath,filetype)
        for remitfile in glob.glob(os.path.join(extractpath, '*.' + filetype)):
            remitfilename = os.path.split(remitfile)[1]
            remitfiledate = str('20' + os.path.split(remitfile)[1][0:6])
            remitfiledate = remitfiledate[0:4] + '-' + remitfiledate[4:6] + '-' + remitfiledate[6:8]
            remitfiledate = time.strptime(remitfiledate, '%Y-%m-%d')
            remitfiledate = datetime.datetime(*remitfiledate[:6])
            remityear = '20' + remitfilename[0:2]
            remitfilenamenoext = string.join(string.split(remitfilename, '.')[0],'')
            filenamepayersuffix = remitfilenamenoext[-2]
            archive(remitarchivepath,remityear,filetype,remitfile,remitfilename)
            if filetype.upper() == 'PLB':
                # copy to \uploadh\[filetype]
                # move to load dir
                try:
                    for plb_payor in fin_plb_payors:
                        if plb_payor in remitfilename:
                            copy(remitfile, os.path.join(financeloadpath,remitfilenamenoext + '.doc'))
                            logfilehandle.write(str(remitfilename) + ' has been moved to ' + str(financeloadpath) + '.\n\n')
                except WindowsError:
                    logfilehandle.write(str(remitfilename) + ' has previously been written to ' + str(financeloadpath) + '.\n\n')
            # move to \uploadh\[filetype]
            try:
                move(remitfile, remitarchivepath)
                logfilehandle.write(str(remitfilename) + ' has been moved to ' + str(remitarchivepath) + '.\n\n')
            except:
                try:
                    move(remitfile, exceptionpath)
                    logfilehandle.write('Exception occurred. ' + str(remitfilename) + ' has been moved to ' + str(exceptionpath) + '.\n\n')
                except:
                    logfilehandle.write('Exception occurred. ' + str(remitfilename) + ' has failed to be moved to ' + str(exceptionpath) + '.\n\n')                        
                                    
def removeoldfiles():
    # remove files from \uploadh\[filetype] that are older than 90 days. files still saved in compressed archive.
    for filetype in dirfiletypelist:
        remitarchivepath = os.path.join(extractpath,filetype)
        for arcfile in glob.glob(os.path.join(remitarchivepath,'*.'+ filetype)):
            arcfiledate = str('20' + os.path.split(arcfile)[1][0:6])
            arcfiledate = arcfiledate[0:4] + '-' + arcfiledate[4:6] + '-' + arcfiledate[6:8]
            arcfiledate = time.strptime(arcfiledate, '%Y-%m-%d')
            arcfiledate = datetime.datetime(*arcfiledate[:6])
            # should try to read file from .zip before removing.
            if (datetime.datetime.today() - arcfiledate).days > 90:
                os.remove(arcfile)

def extractRPTs(extractpath):
    def starttime():
        todaystarttime = (time.localtime()[0],time.localtime()[1],time.localtime()[2])
        if datetime.date.today().weekday() == 0:
            starttime = (time.localtime()[0],time.localtime()[1],time.localtime()[2]-2)
        else:
            starttime = todaystarttime
        return starttime
    starttime = starttime()
    filetype = 'ZPK'
    remitarchivepath = os.path.join(extractpath,'PYR')
    def findtargetzpks(starttime,extractpath):
        targetzpks = []
        for remitfile in glob.glob(os.path.join(extractpath, '*.' + filetype)):
            remitfilename = os.path.split(remitfile)[1]
            filemodtime = time.localtime(os.stat(remitfile).st_mtime)
            if filemodtime > starttime:
                targetzpks.append(remitfile)
        print targetzpks
        return targetzpks
    targetzpks = findtargetzpks(starttime,extractpath)
    for zpk in targetzpks:
        print zpk
        zfile = zipfile.ZipFile(zpk,'r')
        for arcfile in zfile.namelist():
            arcfilename = os.path.basename(arcfile)
            print arcfilename
            if 'bpcs.rpt' in arcfilename:
                BCRPTtext = zfile.read(arcfile)
                loadfilehandler = open(os.path.join(remitarchivepath,arcfilename),'w')
                loadfilehandler.write(BCRPTtext)
                loadfilehandler.close()
                logfilehandle.write(str(arcfilename) + ' has been copied to ' + str(remitarchivepath) + '.\n\n')

movefiles()
removeoldfiles()
#extractRPTs(extractpath)

logfilehandle.write('Job ended at ' + str(datetime.datetime.now().ctime()) + '.\n')
logfilehandle.close()