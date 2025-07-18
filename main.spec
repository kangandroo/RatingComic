# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('utils', './utils')
    ],
    hiddenimports=[
        'pkg_resources.extern',
        'json5',
        'winsdk',
        'PyQt6-Qt6',
        'PyQt6',
        'selenium',
        'numpy',
        'urllib3',
        'torch',
        'torchvision',
        'openpyxl',
        'multiprocessing',
        'multiprocessing.pool',
        'multiprocessing.managers',
        'multiprocessing.popen_spawn_win32', 
        'concurrent.futures',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    uac_admin=True,
    manifest='<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">\n<trustInfo xmlns="urn:schemas-microsoft-com:asm.v3">\n    <security>\n    <requestedPrivileges>\n        <requestedExecutionLevel level="requireAdministrator" uiAccess="false"/>\n    </requestedPrivileges>\n    </security>\n</trustInfo>\n</assembly>\n',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='main',
)