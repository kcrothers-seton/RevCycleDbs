import os, zipfile, glob, datetime, re
from shutil import copy
from shutil import move
from file_hndlrs import File,Txt,Xls
import logging
import traceback

# copy 837 to RCPSI Data Feeds folder
# archive to xp/YYYY/month.zip
# move to xp/YYYY

filetype = '837'

projpath = r'\\txaus.ds.sjhs.com\apps\PFS-DSS\RevCycleDbs\ssifiles'
logpath = os.path.join(projpath,'logs\\837')

xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh'
xfn_ptrn = 'SSI837I*.DAT'

lp = r'\\seton\groupdir\SHN-FTP-XFR\IS-APPS\RCPSI\JDA_837'
test_lp = 'c:\\test\\test'
#lp = test_lp 
jda_fn_prefix = 'SSI837I_'
jda_fn_suffix = '.DAT'

arc_base_p = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\837\\sites'

sites_map = {'A71':{'UMCB':['0015']},
             '271':{'SMCA':['1000'],
                    'SNW':['1100'],
                    'SSW':['1500']},
             'D77':{'DCMC':['2000'],
                    'SMCW':['2100'],
                    'SMCH':['2200']}
             'C77':{'SEBD':['3000','3100','3200','3300','3400'],
                    'SHL':['3500','3600']
             }

class Site():
    def __init__(self,site,ssi_id,region):
        self.site = site
        self.ssi_id = ssi_id
        self.region = region

class site_837(Txt):
    def __init__(self,file):
        Txt.__init__(self,file)
        self.find_date()
        self.find_site_region()
    def find_date(self):
        try:
            dt_str = self.filename_noext[-6:]
            self.dt = datetime.datetime.strptime(dt_str,'%m%d%y')
        except:
            self.dt = None
    def find_site_region(self):
        self.site = None
        self.region = None
        ssi_id = self.filename[7:11]
        for s in sites:
            if s.ssi_id == ssi_id:
                self.site = s.site
                self.region = s.region


class Prcsr():
    def __init__(self):
        self.start()
            
    def start(self):
        global today
        today = datetime.datetime.today()
        self.logger = logging.getLogger('JDA_837')
        log_fn = 'log_837_JDA_' + today.strftime('%Y-%m-%d_%H%M') + '.txt'
        log = os.path.join(logpath,log_fn)
        hdlr = logging.FileHandler(log)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.INFO)
        self.logger.info('Job started.')

    def build_sites(self):
        global sites
        sites = []
        for r, vals in sites_map.items():
            for site,ssi_id in sites_map[r].items():
                s = Site(site,ssi_id,r)
                sites.append(s)
   
    def getx(self):
        self.src_objs = []
        xf_ptrn = os.path.join(xp,xfn_ptrn)
        xfs = glob.glob(xf_ptrn)
        self.logger.info('Files found:\n\n')
        for f in xfs:
            self.logger.info(f + '\n')
            try:
                s = site_837(f)
                self.src_objs.append(s)
            except:
                self.logger.error('Site 837 file identification failure: ' + f + '\n')

    def rename_jda_filenames(self):
        for src_o in self.src_objs:
            try:
                jda_dt_str = src_o.dt.strftime('%Y%m%d')
                jda_fn = jda_fn_prefix + src_o.site + '_' + jda_dt_str + jda_fn_suffix
                jda_f = os.path.join(arc_base_p,jda_fn)
                os.renames(src_o.file,jda_f)
                src_o.file = jda_f
                src_o.filepath,src_o.filename = os.path.split(jda_f)
            except:
                self.logger.error('Rename failure: ' + src_o.filename + '\n')
            

    def archive_src_files(self):
        for src_o in self.src_objs:
            try:
                src_arc_p = os.path.join(arc_base_p,'src',src_o.dt.strftime('%Y'))
                z_fn = 'SSI837I_' + src_o.dt.strftime('%Y-%m') + '.ZIP'
                src_o.archive(src_arc_p,z_fn,remove=False)
            except:
                self.logger.error('archive failure: ' + src_o.filename  + '\n')

    def write_dest_zips(self):
        self.zfs = {}
        # for f in glob.glob(os.path.join(arc_base_p,jda_fn_prefix)):
        for src_o in self.src_objs:
            try:
                zip_fn = src_o.region + '_UB' + src_o.dt.strftime('%Y%m%d') + '.ZIP'
                src_o.archive(arc_base_p,zip_fn)
                self.zfs[zip_fn] = src_o.dt
            except:
                self.logger.error('zip failure: ' + src_o.filename  + '\n')

    def copy_move_zips(self):
        for zfn,dt in self.zfs.items():
            try:
                zf = os.path.join(arc_base_p,zfn)
                copy(zf,lp)
                arc_dest_p = os.path.join(arc_base_p,'dest',dt.strftime('%Y'))
                new_z = os.path.join(arc_dest_p,zfn)
                os.renames(zf,new_z)
            except:
                self.logger.error(traceback.print_exc())
                pass #exception handling
        
    def end(self):
        self.logger.info('Job ended.')
        self.logger.handlers = []

p = Prcsr()
p.build_sites()
p.getx()
p.archive_src_files()
p.rename_jda_filenames()
p.write_dest_zips()
p.copy_move_zips()
p.end()