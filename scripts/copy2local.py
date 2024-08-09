import sys
import os

if len(sys.argv) == 0:
    print('Usage: copy2local.py filename')
    sys.exit(2)

for arg in sys.argv[1:]:
    print(arg)
    tmpname = f"{arg}.tmp"
    with open(arg, encoding='utf-8') as src:
        with open(tmpname, 'w', encoding='utf-8') as dst:
            line = src.readline()
            while line:
                ld = line.split()
                if len(ld) > 3 and ld[0].upper() == "COPY" and \
                        ld[2].upper() in {'FROM', 'TO'} and ld[3].upper() != 'STDIN':
                    l1 = f"\\set command '\\\\copy {ld[1]} {ld[2]} ' {ld[3]} " + " ".join(
                        ld[4:]) + "\n"
                    print(l1)
                    dst.write(l1)
                    dst.write(':command\n')
                else:
                    dst.write(line)
                line = src.readline()
    os.unlink(arg)
    os.link(tmpname, arg)
    os.unlink(tmpname)
