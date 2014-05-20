import os, zipfile, glob, datetime, re
from shutil import copy
from file_hndlrs import File,Txt,Xls
import logging

# copy 837 to RCPSI Data Feeds folder
# archive to xp/YYYY/month.zip
# move to xp/YYYY

filetype = '837'

projpath = 'N:\\PFS-DSS\\RevCycleDbs\\ssifiles'
logpath = os.path.join(projpath,'logs\\837')
reclogpath = 'G:\\RevenueCycle\\Reporting\\RCPSI\\RIC 837 Claim Count Reconciliation'


xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\' + filetype 
xfn_ptrn = 'daily-837*.zip'

lp_base = 'G:\\SHN-FTP-XFR\\IS-Apps\\RCPSI'
lp = os.path.join(lp_base,filetype)
test_lp = 'c:\\test\\test'
#lp = test_lp 

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

def end():
    logger.info('Job ended.')
    logger.handlers = []

def find_zipfile(xz_dt):
    zipname = xz_dt.strftime('%Y%m') + '_837.zip'
    zip_full_fn = os.path.join(arc_path, zipname)
    if os.path.exists(zip_full_fn):
        zloadfile = zipfile.ZipFile(zip_full_fn,'r')
        lfns = zloadfile.namelist()
        zloadfile.close()
        zloadfile = zipfile.ZipFile(zip_full_fn,'a')
    else:
        zloadfile = zipfile.ZipFile(zip_full_fn,'w')
        lfns = []
    return zloadfile,lfns,zipname

def increment(inc):
    inc1,inc2 = inc[0],inc[1]
    if ord(inc2) == 90:
        inc1 = chr(ord(inc1)+1)
        inc2 = chr(65)
    else:
        inc2 = chr(ord(inc2)+1)
    new_inc = inc1 + inc2
    return new_inc

def build_load_fn(fn_837,xz_dt_str,lfns,zipname):
    # fn_837 = h65000ac.837
    # l_fn = H00001AA_12312013.837
    fn_837_noext = fn_837.split('.')[0]
    inc = fn_837_noext[-2:].upper() # AA
    remote = fn_837_noext[:-2]
    l_fn_cand = remote + inc + '_' + xz_dt_str + '.837'
    while l_fn_cand in lfns:
        inc = increment(inc)
        l_fn_cand = remote + inc + '_' + xz_dt_str + '.837'
    lfns.append(l_fn_cand)
    return l_fn_cand,lfns

def load(zloadfile,xz_file,l_fn,x_ft,zfile):
    # write a new file with the new name to loadpath
    # write new file to zip file
    # remove new file from loadpath
    loadfile = os.path.join(arc_path,l_fn)
    loadfilehandler = open(loadfile, 'w')
    loadfilehandler.write(x_ft)
    loadfilehandler.close()
    zloadfile.write(loadfile,l_fn,zipfile.ZIP_DEFLATED)
    os.remove(loadfile)
    return None


def find_claim_count(x_ft):
    matchlist = re.findall('CLM',x_ft)
    claimcount = len(matchlist)
    return claimcount


def log_claim_file_counts(loadfilecount,claimcount,xz_fn,xz_dt):
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
        headers = ['Date','File Count','Claim Count','File Name']
        rec_log_o.load_headers(headers)
    blank_row = rec_log_o.find_first_blank_row()
    loadvals = [xz_dt.strftime('%m/%d/%Y'),loadfilecount,claimcount,xz_fn]
    for i,lv in enumerate(loadvals):
        rec_log_o.loadvalue(lv,blank_row,i+1)
    rec_log_o.saveclosequit()
    return None


logger = start()
xz_dt_ptrn = re.compile(r'\d\d\d\d\d\d')
xz_ptrn = os.path.join(xp,xfn_ptrn)
xz_files = glob.glob(xz_ptrn)
for xz_file in xz_files:
    try:
        loadfilecount = 0
        claimcount = 0
        xz_fn = os.path.basename(xz_file)
        # xz_file = daily-837aa-090512.zip
        m = re.search(xz_dt_ptrn,xz_file)
        xz_dt = datetime.datetime.strptime(m.group(),'%m%d%y')
        arc_path = os.path.join(xp,xz_dt.strftime('%Y'))
        zloadfile,lfns,zipname = find_zipfile(xz_dt)
        xz_dt_str = xz_dt.strftime('%m%d%y') #09052012
        zfile = zipfile.ZipFile(xz_file,'r')
        for fn_837 in zfile.namelist():
            loadfilecount += 1
            # fn_837 = h65000ac.837
            l_fn,lfns = build_load_fn(fn_837,xz_dt_str,lfns,zipname)
            x_ft = zfile.read(fn_837)
            load(zloadfile,xz_file,l_fn,x_ft,zfile)
            claimcount += find_claim_count(x_ft)
        zfile.close()
        zloadfile.close()
        copy(xz_file,arc_path)
        os.rename(xz_file,os.path.join(lp,xz_fn))
        log_claim_file_counts(loadfilecount,claimcount,xz_fn,xz_dt)
    except:
        logger.error('fail: ' + xz_file)

