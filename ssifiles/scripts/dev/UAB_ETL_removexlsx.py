import os, zipfile, datetime, re, glob, traceback
from shutil import move
from file_hndlrs import File,Xls

xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\837'
xfn_ptrn = 'UAB_*.zip'

arc_p = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\UAB'
l_p = 'G:\\RevenueCycle\\Reporting\\Billing\\Unable To Bill'

projpath = 'N:\\PFS-DSS\\RevCycleDbs\\ssifiles'
logpath = os.path.join(projpath,'logs\\UAB')

sheet_order = ['UABSummary','CurrentDayDetail','PriorDayDetail',
               'UABTrendingByDateSorted','UABSummaryBySite','Report Descriptions']

def move_UAB_zips():
    UAB_zips = glob.glob(os.path.join(xp,xfn_ptrn))
    new_UAB_zips = []
    for z in UAB_zips:
        fn = os.path.basename(z)
        new_z = os.path.join(arc_p,fn)
        move(z,new_z)
        new_UAB_zips.append(new_z)
    return new_UAB_zips


def find_UAB_date(UAB_fn):
    try:
        m = re.search('\d\d\d\d\d\d\d\d',UAB_fn)
        dt_str = m.group()
        z_dt = datetime.datetime.strptime(dt_str,'%Y%m%d')
    except:
        z_dt = None
    return z_dt


def combine_sheets(nm_list,z_dt):
    l_fn = 'UAB_' + z_dt.strftime('%m%d%y') + '.xls'
    l_f = os.path.join(l_p,l_fn)
    if os.path.isfile(l_f):
        os.remove(l_f)
    lf_o = Xls(l_f)
    for fn in nm_list:
        f = os.path.join(arc_p,fn)
        sf = Xls(f)
        sht = sf.xls.Worksheets(1)
        sht.Move(lf_o.wkbk.Worksheets(1))
        sht_name = os.path.splitext(fn)[0]
        lf_o.wkbk.ActiveSheet.Name = sht_name
        os.remove(f)
    f2 = os.path.join(l_p,'_Report Descriptions.xls')
    sf2 = Xls(f2)
    sht2 = sf2.xls.Worksheets(1)
    sht2.Move(lf_o.wkbk.Worksheets(1))
    lf_o.saveclosequit(close=False)
    return lf_o
        
def order_sheets(lf_o):
    for i,sht_nm in enumerate(sheet_order):
        sht = lf_o.xls.Worksheets(sht_nm)
        sht.Move(lf_o.wkbk.Worksheets(i+1))
    lf_o.wkbk.Worksheets(sheet_order[0]).Activate()
    lf_o.saveclosequit(close=False)
    return None

def delete_sheets(lf_o):
    delete_sheets = ['Sheet1','Sheet2','Sheet3']
    for sht in delete_sheets:
        lf_o.wkbk.Worksheets(sht).Delete() # test syntax
    lf_o.saveclosequit()
    return None

def extract_zip(new_UAB_zips):
    zfile = zipfile.ZipFile(z,'r')
    nm_list = zfile.namelist()
    for xf in nm_list:
        xb = zfile.read(xf)
        f = os.path.join(arc_p,xf)
        if os.path.isfile(f):
            os.remove(f)
        xf_hndlr = open(f,'wb')
        xf_hndlr.write(xb)
    return nm_list

def clean_up():
    fns = [sht_nm + '.xlsx' for sht_nm in sheet_order]
    # remove zip file contents automatically extracted during download process.
    for fn in fns:
        f = os.path.join(xp,fn)
        if os.path.isfile(f):
            os.remove(f)
    arc_old_xls()
    return None

def arc_old_xls():
    if datetime.datetime.today().weekday() in [3]: #[5,6]:
        age_threshhold = 60
        arc_basepath = os.path.join(l_p,'archive')
        dt_threshhold = datetime.datetime.today() - datetime.timedelta(days=age_threshhold)
        UAB_files = glob.glob(os.path.join(l_p,'UAB*.xls'))
        for uab_f in UAB_files:
            try:
                uab_fo = File(uab_f)
                mmddyy = uab_fo.filename_noext[-6:]
                dt = datetime.datetime.strptime(mmddyy,'%m%d%y')
                if dt < dt_threshhold:
                    arc_path = os.path.join(arc_basepath,dt.strftime('%Y'))
                    zipfullfilename = 'UAB_' + dt.strftime('%Y-%m') + '.zip'
                    uab_fo.archive(arc_path,zipfullfilename)
            except:
                print 'file: ' + uab_f
                traceback.print_exc()
    return None


new_UAB_zips = move_UAB_zips()
for z in new_UAB_zips:
    UAB_fn = os.path.basename(z)
    z_dt = find_UAB_date(UAB_fn)
    if z_dt:
        nm_list = extract_zip(z)
        lf_o = combine_sheets(nm_list,z_dt)
        order_sheets(lf_o)
        delete_sheets(lf_o)

clean_up()
