import os, subprocess


def bin_copy(src, dest):
    try:
        subprocess.check_call(['/bin/cp', src, dest])
    except subprocess.CalledProcessError:
        raise OSError("Copy failed %s %s" % (src, dest))

def bin_mv(src, dest):
    try:
        subprocess.check_call(['/bin/mv', src, dest])
    except subprocess.CalledProcessError:
        raise OSError("Move failed %s %s" % (src, dest))

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("usage: <src> <dest>")
    
    src, dest = sys.argv[1:]
    
    if not os.path.exists(src): raise OSError("missing src file")

    bin_copy(src, dest)

    
