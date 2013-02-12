perl -ane 'if($F[0] eq "<s>") {print} elsif($F[2] eq "VVG" && $F[5] eq "NMOD"){$F[2]="JJ"; $F[1] = $F[0]; print join("\t", @F)."\n"}else{print;}'
