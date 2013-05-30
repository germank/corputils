#!/usr/bin/env python
import argparse
import fileinput

def main():
    parser = argparse.ArgumentParser('Translates a DP into a DOT graph')
    parser.add_argument('dp_file')

    args = parser.parse_args()

    i = 0
    for l in fileinput.input(args.dp_file):
        if l == '<s>\n':
            print "digraph sentence_{0} {{".format(i)
            label = ""
        elif l == '</s>\n':
            print 'label = "{0}"'.format(label.strip())
            print '}'
            i+=1
        elif not l.startswith("<"):
            word,lemma,pos,id,dep_id,dep_tag = l.split()
            label = label + " " + word
            print 'n_{0}[label="{1}"];'.format(id, word+"/"+pos)
            print 'n_{0} -> n_{1} [label="{2}"];'.format(id, dep_id, dep_tag)

if __name__ == '__main__':
    main()
