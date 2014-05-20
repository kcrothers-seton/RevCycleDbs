import os, glob, string, zipfile, datetime,ftplib
from shutil import move

filetype = '835'
xpath = 'N:\\pfs-dss\\RevCycleDBs\\PayNav\\ETL\\transform'

site = 'ftp.surepayhealth.net'
id = 'sfhnupload'
pw = 'ruSwEY7b'

loaddir = 'Inbound//ClaimData//'+ filetype +'Data'

arc_path = 'G:\\SHN-FTP-XFR\\IS-Apps\\PayNav\\' + filetype + '\\archive'

f_ptrn = '*.' + filetype

logpath = 'N:\\PFS-DSS\\RevCycleDbs\\PayNav\\logs'

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

def transform():
    xfiles = glob.glob(os.path.join(xpath,f_ptrn))
    zipfilename = today.strftime('%y%m%d') + '_' + filetype + 's.zip'
    for f in xfiles:
        fn = os.path.basename(f)
        f_obj = File(xpath,fn)
        f_obj.archive(xpath,zipfilename)
    transform_file = os.path.join(xpath,zipfilename)
    return transform_file


# find files modified since yesterday morning
# check to see if file already loaded.
# if not, load

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
                log.write('file upload failure for: ' + str(failed_files) + '\n\n')
        if confirmed_files:
            log.write('file upload confirmed for: ' + str(confirmed_files) + '\n\n')
            for f in confirmed_files:
                move(f,arc_path)
        ftp.quit()
    return None

log = Txt(logpath,'log_' + filetype + '_' + datetime.datetime.today().strftime('%Y-%m-%d_%H%M') + '.txt')
log.write('job started at ' + datetime.datetime.now().ctime() + '\n\n')

today = datetime.datetime.today()
transform_file = transform()
target_files = [transform_file]
# use a list rather than individual file to make process consistent with standard ftp upload function.
load(target_files)

log.write('job ended at ' + datetime.datetime.now().ctime() + '\n\n')
