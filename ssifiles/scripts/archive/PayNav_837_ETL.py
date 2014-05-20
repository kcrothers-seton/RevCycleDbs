import os, zipfile, glob, string, datetime, re,ftplib
from shutil import copy

filetype = '837'

xpath = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\' + filetype 
alt_xpath = os.path.join(xpath,'archive')
arc_path = 'G:\\SHN-FTP-XFR\\IS-Apps\\PayNav\\'+ filetype+'\\archive'


site = 'ftp.surepayhealth.net'
id = 'sfhnupload'
pw = 'ruSwEY7b'

loaddir = 'Inbound//ClaimData//'+ filetype +'Data'


projpath = 'N:\\PFS-DSS\\RevCycleDbs\\PayNav'
logpath = os.path.join(projpath,'logs')


class File():
    def __init__(self,filepath,filename):
        self.filepath = filepath
        self.filename = filename
        self.file = os.path.join(filepath,filename)
        self.filetype = string.split(self.filename,'.',)[-1]        
        self.filenamenoext = string.join(string.split(self.filename,'.',)[:-1],'')
        
    def archive(self,archivepath,zipfilename):
        zipfullfilename = os.path.join(archivepath,zipfilename)
        try:
            zfile = zipfile.ZipFile(zipfullfilename, 'r')
        except IOError, BadZipfile:
            try:
                zfile = zipfile.ZipFile(zipfullfilename, 'w')
                zfile.write(self.file, self.filename, zipfile.ZIP_DEFLATED)
                zfile.close()
                os.remove(self.file)
            except IOError, BadZipfile:
                pass
        else:
            try:
                zfile = zipfile.ZipFile(zipfullfilename, 'a')
                zfile.write(self.file, self.filename, zipfile.ZIP_DEFLATED)
                zfile.close()
                os.remove(self.file)
            except IOError, BadZipfile:
                pass


class Txt(File):
    def filetext(self):
        self.filereader = open(self.file,'r')
        self.filetext = self.filereader.read()
        self.filereader.close()
    def filelines(self):
        self.filereader = open(self.file,'r')
        self.filelines = self.filereader.readlines()
        self.filereader.close()
    def write(self,text):
        if os.path.isfile(self.file):
            hndlr = open(self.file,'a')
        else:
            hndlr = open(self.file,'w')            
        hndlr.write(text)
        hndlr.close()
    def writelines(self,text):
        if os.path.isfile(self.file):
            hndlr = open(self.file,'a')
        else:
            hndlr = open(self.file,'w')            
        hndlr.writelines(text)
        hndlr.close()


def find_files_bydate(targetpath,fn_ptrn,datesection_start,datesection_stop,date_ptrn,date_threshhold,operator):
    targetfiles = []
    searchcandidatefilelist = glob.glob(os.path.join(targetpath,fn_ptrn))
    for searchcandidatefile in searchcandidatefilelist:
        filename = os.path.basename(searchcandidatefile)
        try:
            filedate = datetime.datetime.strptime(filename[datesection_start:datesection_stop],date_ptrn).date()
            if operator == '==':
                if filedate == date_threshhold:
                    targetfiles.append(searchcandidatefile)
            elif operator == '>':
                if filedate > date_threshhold:
                    targetfiles.append(searchcandidatefile)
            elif operator == '<':
                if filedate < date_threshhold:
                    targetfiles.append(searchcandidatefile)          
        except:
            pass    
    return targetfiles
    
# check for prior month folder
# and create prior month folder if not exist


def find_extdt():    
    """
    Files are produced at SSI Monday through Friday.
    Files are put on the gateway one day after production.
    Files are dated for the day they are put on the gateway,
    hence files are dated Tues - Sat.
    Tues - Fri the file to be picked up will be named with the current date.
    On Monday the file will be dated with the previous Saturday.
    """
    daynum = datetime.date.today().weekday()
    if daynum == 0:
        ext_dt = datetime.date.today() - datetime.timedelta(days=2)
    else:
        ext_dt = datetime.date.today()
    return ext_dt

def load(target_files):

    def upload(ftp,target_files):
        for f in target_files:
            fn = os.path.basename(f)
            ext = os.path.splitext(f)[1]
            if ext in (".txt", ".htm", ".html"):
                ftp.storlines("STOR " + fn, open(f))
            else:
                ftp.storbinary("STOR " + fn, open(f, "rb"), 1024)

    def retrieve_dirlist(ftp):
        data = []
        ftp.dir(data.append)
        return data

    def confirm_upload(ftp,target_files):
        confirmed_files = []
        failed_files = []
        found_files = []    
        data = retrieve_dirlist(ftp)
        for line in data:
            fn = line.split(' ')[-1]
            found_files.append(fn)
        for f in target_files:
            fn = os.path.basename(f)
            if fn in found_files:
                confirmed_files.append(f)
            else:
                failed_files.append(f)
        return confirmed_files,failed_files
    

    if target_files:
        ftp = ftplib.FTP(site)
        ftp.login(id, pw)
        ftp.cwd(loaddir)
        upload(ftp,target_files)
        confirmed_files,failed_files = confirm_upload(ftp,target_files)
        if failed_files:
                log.write('file upload failure for: ' + str(failed_files)+ '\n\n')
        if confirmed_files:
            log.write('file upload confirmed for: ' + str(confirmed_files)+ '\n\n')
            for f in confirmed_files:
                copy(f,arc_path)
        ftp.quit()

def clean_up():
    date_threshhold = datetime.date.today() - datetime.timedelta(days=60)
    targetfiles = find_files_bydate(alt_xpath ,'*.ZIP',-10,-4,'%m%d%y',date_threshhold,'<')
    for f in targetfiles:
        fn = os.path.basename(f)
        filedate = datetime.datetime.strptime(fn[-10:-4],'%m%d%y').date()
        monthdir = filedate.strftime('%Y-%m')
        monthdirpath = os.path.join(alt_xpath,monthdir)
        if not os.path.exists(monthdirpath):
            os.makedirs(monthdirpath)
        os.rename(os.path.join(alt_xpath,fn),os.path.join(monthdirpath,fn))
    return None

log = Txt(logpath,'log_' + filetype + '_' + datetime.datetime.today().strftime('%Y-%m-%d_%H%M') + '.txt')
log.write('job started at ' + datetime.datetime.now().ctime() + '\n\n')

ext_dt = find_extdt()
load_files = find_files_bydate(xpath,'*.ZIP',-10,-4,'%m%d%y',ext_dt,'==')
if not load_files:
    load_files = find_files_bydate(alt_xpath,'*.ZIP',-10,-4,'%m%d%y',ext_dt,'==')
load(load_files)
clean_up()
log.write('job ended at ' + datetime.datetime.now().ctime()+ '\n\n')
