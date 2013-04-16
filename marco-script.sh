#!/bin/bash
perl -ane 'if ($F[0] eq "<s>"){print}else{$pos=lc(substr($F[2],0,1)); print $F[1],"-",$pos,"\n"}' |\
# #preprocess corpus stream to get nouns, verbs, adj and adv, getting rid #of strange symbols
~/print_directional_bigrams.pl -p -d"<s>" -w $1 $2 - # |\
#gawk '{print $1 "\t" $3}' |\
# merge "dog l cat" and "dog r cat"
#sort -T . | uniq -c | gawk '{print $2 "\t" $3 "\t" $1}'
