import os, zipfile, glob, datetime, re
from shutil import copy
from shutil import move
from file_hndlrs import File,Txt,Xls
import logging
from job_tools import set_test_paths

# copy 837 to RCPSI Data Feeds folder
# archive to xp/YYYY/month.zip
# move to xp/YYYY

filetype = '837'

projpath = r'\\txaus.ds.sjhs.com\apps\PFS-DSS\RevCycleDbs\ssifiles'
logpath = os.path.join(projpath,'logs\\837')
reclogpath = '\\\\seton\\groupdir\\RevenueCycle\\Reporting\\RCPSI\\RIC 837 Claim Count Reconciliation'
    

xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\' + filetype 
xfn_ptrn = 'daily-837*.zip'

lps = [r'\\seton\groupdir\SHN-FTP-XFR\IS-Apps\RCPSI\837',
       r'\\seton\groupdir\shn-ftp-xfr\IS-Apps\E-Scan']

lp_medenterprise = r'\\seton\groupdir\SHN-FTP-XFR\IS-Apps\RCPSI\MedEnterprise_837'

def start():
    global today
    today = datetime.datetime.today()
    logger = logging.getLogger('RAC837')
    log_fn = 'log_837_' + today.strftime('%Y-%m-%d_%H%M') + '.txt'
    log = os.path.join(logpath,log_fn)
    hdlr = logging.FileHandler(log)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    logger.info('Job started.')
    global lfns
    lfns = {}
    return logger


def increment(inc):
    inc1,inc2 = inc[0],inc[1]
    if ord(inc2) == 90:
        inc1 = chr(ord(inc1)+1)
        inc2 = chr(65)
    else:
        inc2 = chr(ord(inc2)+1)
    new_inc = inc1 + inc2
    return new_inc


def log_file_count(loadfilecount,xz_fn,xz_dt):
    # not used
    rec_log_fn = '837 RIC Receipt Log.xls'
    rec_log = os.path.join(reclogpath,rec_log_fn)
    rec_log_o = Xls(rec_log)
    wksht = 'Sent ' + xz_dt.strftime('%Y-%m')
    try:
        rec_log_o.xls.ActiveWorkbook.Worksheets(wksht).Activate()
    except:
        # create new worksheet
        rec_log_o.xls.Worksheets.Add()
        rec_log_o.xls.ActiveSheet.Name = wksht
        headers = ['Date','File Count','File Name']
        rec_log_o.load_headers(headers)
    blank_row = rec_log_o.find_first_blank_row()
    loadvals = [xz_dt.strftime('%m/%d/%Y'),loadfilecount,xz_fn]
    for i,lv in enumerate(loadvals):
        rec_log_o.loadvalue(lv,blank_row,i+1)
    rec_log_o.saveclosequit()
    return None

def find_xzfiles():
    xz_ptrn = os.path.join(xp,xfn_ptrn)
    xz_files = glob.glob(xz_ptrn)
    return xzfiles

class Xz(File3):
    def __init__(self,file):
        File3.__init__(file)
        self.zfile = zipfile.ZipFile(file,'r')
        self.find_dt()
    def find_dt(self):
        self.dt = None
        self.dt_ptrn = re.compile(r'\d\d\d\d\d\d')
        m = re.search(self.dt_ptrn,self.filename)
        if m:
            self.dt = datetime.datetime.strptime(m.group(),'%m%d%y')


class MonthArchiveFile(File3):
    def __init__(self,xz):
        self.arc_path = os.path.join(xp,xz.dt.strftime('%Y'))
        fn = xz.dt.strftime('%Y%m') + '_837.zip'
        f = os.path.join(arc_path, fn)
        File3.__init__(f)
        self.find_montharchive(xz.dt)
        self.loadfilecount = 0

    def find_montharchive(self,xz_dt):
        if os.path.exists(self.file):
            zfile = zipfile.ZipFile(self.file,'r')
            self.lfns = zfile.namelist()
            zfile.close()
            self.zfile = zipfile.ZipFile(self.file,'a')
        else:
            if not os.path.exists(arc_path):
                os.makedirs(arc_path)
            self.zfile = zipfile.ZipFile(zip_full_fn,'w')
            self.lfns = []

    def build_load_fn(self,fn_837,xz):
        # fn_837 = h65000ac.837
        # l_fn = H00001AA_12312013.837
        xz_dt_str = xz.dt.strftime('%m%d%y') #09052012
        fn_837_noext = fn_837.split('.')[0]
        inc = fn_837_noext[-2:].upper() # AA
        remote = fn_837_noext[:-2]
        l_fn_cand = remote + inc + '_' + xz_dt_str + '.837'
        while l_fn_cand in self.lfns:
            inc = increment(inc)
            l_fn_cand = remote + inc + '_' + xz_dt_str + '.837'
        l_fn = l_fn_cand 
        self.lfns.append(l_fn)
        return l_fn

    def load_837_file(self,fn_837,xz):
        # fn_837 = h65000ac.837
        l_fn = build_load_fn(fn_837,xz)
        x_ft = xz.zfile.read(fn_837)
        self.zfile.write_str(x_ft,l_fn,zipfile.ZIP_DEFLATED)
        self.loadfilecount += 1
        

def archive_837(xz):
    arcf = MonthArchiveFile(xz.dt)
    for fn_837 in xz.zfile.namelist():
        arcf.load_837_file(fn_837,xz)
    arcf.zfile.close()

def load_medenterprise(xz):
    excluded_payers = ['00011','00012']
    lfn = xz.filename_noext + '_MedEnterprise.zip'
    lf = os.path.join(xp,lfn)
    zfile = zipfile.ZipFile(lf,'w')
    for fn_837 in xz.zfile.namelist():
        epcheck = False
        for ep in excluded_payers:
            if ep in fn_837:
                epcheck = True
                break
        if not epcheck:
            x_ft = xz.zfile.read(fn_837)
            zfile.writestr(x_ft,fn_837)
    # zipfile must have all files written to it before being moved to loadpath
    # consuming job will try to move file as soon as it is written to loadpath
    # and before all files are written to it.
    zfile.close()
    move(lf,lp_medenterprise)


def end():
    logger.info('Job ended.')
    logger.handlers = []


#xp,lps = set_test_paths(xp,lps)
logger = start()
xzfiles = find_xzfiles()
for xz_file in xz_files:
    xz = Xz(xz_file)
    for lp in lps:
        lf = os.path.join(lp,xz.filename)
        copy(xz_file,lf)
    load_medenterprise(xz)
    archive_837(xz)
    move(xz.file,arc_path)
    xz.zfile.close()
    #log_file_count(loadfilecount,xz_fn,xz_dt)
end()
