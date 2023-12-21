# -*- mode: python ; coding: utf-8 -*-

block_cipher = None
from PyInstaller.utils.hooks import collect_dynamic_libs, collect_data_files

# pyinstaller --distpath . NTD_05_main_GUI.spec
import certifi
import pyproj
import pyarrow
import pyarrow.vendored
from pathlib import Path

datalist = [('lib\dot.png', 'lib'), ('lib\ShapeFiles', 'lib\ShapeFiles'), 
                ('lib\ShapeFilesCSV', 'lib\ShapeFilesCSV'), ('lib\pyzmq.libs', 'pyzmq.libs'), 
                ('lib\pyproj.libs', 'pyproj.libs'), ('lib\shapely.libs', 'shapely.libs'),
                ('lib\\pandas.libs', 'pandas.libs'), ('lib\\numpy.libs', 'numpy.libs')]
datalist.extend(collect_data_files("certifi"))
datalist.extend(collect_data_files("pyproj"))
datalist.extend(collect_data_files("pyarrow"))
datalist.extend(collect_data_files("pyarrow.vendored", include_py_files=True))
print(datalist)

a = Analysis(['NTD_05_main_GUI.py'],
             pathex=['C:\\Users\\William.Chupp\\OneDrive - DOT OST\\Documents\\DANAToolTesting\\FHWA-DANATool',
                     'C:\\Users\\William.Chupp\\Anaconda3\\envs\\dana_env\\Lib\\site-packages'],
             binaries=collect_dynamic_libs("pyarrow", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib', '*.pyi']) 
                + collect_dynamic_libs("regex", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib']) 
                + collect_dynamic_libs("zmq", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib']) 
                + collect_dynamic_libs("rtree", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib']) 
                + collect_dynamic_libs("pyproj", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib']) 
                + collect_dynamic_libs("shapely", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib'])
                + collect_dynamic_libs("sklearn", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib'])
                + collect_dynamic_libs("numpy", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib'])
                + collect_dynamic_libs("pandas", search_patterns=['*.dll', '*.dylib', 'lib*.so', '*.pyd', '*.lib']),
             datas=datalist,
             hiddenimports=['fiona._shim', 'fiona.schema', 'babel.numbers', 'shapely._geos', 'appdirs'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

splash = Splash('SpashScreen.jpg', 
                binaries=a.binaries,
                datas=a.datas,
                text_pos=(7, 300),
                text_color='white',
                always_on_top=False)
exe = EXE(pyz,
          splash,
          splash.binaries,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='DANATool',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=False,
          icon='.\\lib\\dot.ico')
