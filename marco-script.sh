zcat ~/uncompressed-data/*xml.gz | perl -ane 'if ($F[0] eq "<s>"){print}elsif ($F[2]=~/^(NN|VV|RB|JJ)/ && $F[1]=~/^[A-Za-z\-]+$/ && $F[1]=~/^[A-Za-z]/ && $F[1]=~/[A-Za-z]$/ && $F[1]!~/\-\-/){$pos=lc(substr($F[2],0,1)); print $F[1],"-",$pos,"\n"}'|\ # #preprocess corpus stream to get nouns, verbs, adj and adv, getting rid #of strange symbols
~/model-preparation/print_directional_bigrams.pl -p -d"<s>" -w300 pivots.txt - |\
gawk '{print $1 "\t" $3}' |\ # merge "dog l cat" and "dog r cat"
sort -T . | uniq -c | gawk '{print $2 "\t" $3 "\t" $1}'
