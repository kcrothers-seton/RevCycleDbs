# remit file move

# next version:
    # line 50ish: should try to read file from .zip before removing.
    # function for compressing!

import os, string, datetime, time, glob, zipfile
from shutil import copy
from shutil import move
import win32com.client


extractpath = '\\\\AUSSSI\\SSIAPP\\RENWS\\BILLING\\UPLOADH'
financeloadpath = 'G:\\Finance\\Reimbursement\\Pass_through_reports'
logfilepath = 'N:\\PFS-DSS\\revcycledbs\\ssifiles\\logs'

dirfiletypelist = ['DD2','PLB', 'PYR','RP1']
payernamelist = []

logfile = os.path.join(logfilepath, 'log_' + datetime.datetime.today().strftime('%Y-%m%-%d_%H%M') + '.TXT')

logfilehandle = open(logfile, 'w')
logfilehandle.write('Job started at ' + str(datetime.datetime.now().ctime()) + '.\n')
# script log file documentation as function
    # def (remitfile, job type, destination)
        # write(remitfile + ' has been '+ jobaction + ' to ' + loadpath + '.\n')

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
        if filetype.upper() == 'PLB':
# compress
            try:
                zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' + '.zip'),'r')
            except IOError, BadZipfile:
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
# copy to \uploadh\[filetype]
            copy(remitfile, remitarchivepath)
            logfilehandle.write(str(remitfilename) + ' has been copied to ' + str(remitarchivepath) + '.\n')
# move to load dir
            try:
                os.rename(remitfile, os.path.join(financeloadpath,remitfilenamenoext + '.doc'))
                logfilehandle.write(str(remitfilename) + ' has been moved to ' + str(financeloadpath) + '.\n\n')
            except WindowsError:
                os.remove(remitfile)
                logfilehandle.write(str(remitfilename) + ' has previously been written to ' + str(financeloadpath) + '.\n\n')

        elif filetype.upper() == 'DD2':
# compress
            try:
                zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' + '.zip'),'r')
            except IOError, BadZipfile:
                zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'w')
                zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
                logfilehandle.write(str(remitfilename) + ' has been compressed to ' + str(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip')) + '.\n\n')
                zfile.close()
            else:
                try:
                    zfile.read(remitfile)
                except:
                    zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'a')
                    zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
                    logfilehandle.write(str(remitfilename) + ' has been compressed to ' + str(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip')) + '.\n\n')
                    zfile.close()
        elif filetype.upper() in dirfiletypelist: # this will only select file types in list that aren't 835's, plb's or cnp's: dds, rp1s, pyrs
# compress
            try:
                zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' + '.zip'),'r')
            except IOError, BadZipfile:
                zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'w')
                zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
                logfilehandle.write(str(remitfilename) + ' has been compressed to ' + str(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip')) + '.\n\n')
                zfile.close()
            else:
                try:
                    zfile.read(remitfile)
                except:
                    zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'a')
                    zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
                    logfilehandle.write(str(remitfilename) + ' has been compressed to ' + str(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip')) + '.\n\n')
                    zfile.close()
# move to \uploadh\[filetype]
            move(remitfile, remitarchivepath)
            logfilehandle.write(str(remitfilename) + ' has been moved to ' + str(remitarchivepath) + '.\n\n')
# remove files from \uploadh\[filetype] that are older than 90 days. files still saved in compressed archive.
    for arcfile in glob.glob(os.path.join(remitarchivepath,'*.'+ filetype)):
        arcfiledate = str('20' + os.path.split(arcfile)[1][0:6])
        arcfiledate = arcfiledate[0:4] + '-' + arcfiledate[4:6] + '-' + arcfiledate[6:8]
        arcfiledate = time.strptime(arcfiledate, '%Y-%m-%d')
        arcfiledate = datetime.datetime(*arcfiledate[:6])
        # should try to read file from .zip before removing.
        if (datetime.datetime.today() - arcfiledate).days > 90:
            os.remove(arcfile)
            
logfilehandle.write('Job ended at ' + str(datetime.datetime.now().ctime()) + '.\n')