#! /opt/python/bin/python2.7

import sys
import re
import fileinput

# This ad-hoc script assumes that input is in our current (as of Sep
# 18 2013) tree-tagged, maltparsed corpus format, and it changes it so
# that if a noun (common or proper: N.*) depends on a "predicate
# chain", its head offset will be updated to point to the last item in
# the predicate chain. A predicate chain is a sequence of verbs and/or
# adjectives (JJ.*, MD, V.*) such that each element depends on the
# previous one in the chain. Nothing else is changed






# the following function is called after we have seen a full sentence:
# it updates the noun head offsets if needed, and prints
def process_sentence(original_sentence_lines,predicate_chain_dependent_offsets):
 
 # each line in sentence array will be in tab-delimited format:
 # word lemma pos offset head_offset dep_rel
 
 for line in original_sentence_lines:
  # heavy processing only concerns nouns (restricted to common and
  # proper nouns, no pronouns and the like), as we might need to update
  # their head_offset
  matchednoun = re.match("^([^\t]+\t[^\t]+\tN[^\t]+\t[^\t]+\t)([^\t]+)(.*)$",line)
  if matchednoun:
   # we split the string into a first part, the head_offset (that we
   # might need to change), and the rest
   prefix = matchednoun.group(1)
   head_offset = int(matchednoun.group(2))
   suffix = matchednoun.group(3)

   # we extract the current offset from the prefix
   curr_offset = int(re.sub("^.*\t([^\t]+)\t$",r"\1",prefix))


   # we start by assuming that the "real" head is the current head
   real_head_offset = head_offset

   # if head precedes the current noun, we do not change anything
   if real_head_offset>curr_offset:
    # however, if the current head linearly follows the noun and it is
    # part of a predicate chain, we go down the predicate chain
    # dependents and pick the offset of the last one as "real" head
    # for the noun
    while (predicate_chain_dependent_offsets[real_head_offset-1] > 0):
     real_head_offset = predicate_chain_dependent_offsets[real_head_offset-1]

#    # debug: uncomment to see which names have changed offsets, which stay the same
#    if real_head_offset != head_offset:
#     real_head_offset = "CX_" + str(real_head_offset)
#    else:
#     real_head_offset = "SA_" + str(real_head_offset)

   # now, we print the original line with the updated offset
   sys.stdout.write(prefix + str(real_head_offset) + suffix + "\n")

  else: # if not a noun, just print original line as is
   sys.stdout.write(line)

 return # end of process_sentence() definition



# here we initialize the lists we will need in the "main"

# list to hold the lines in the original input
original_sentence_lines = []
# list keeping track of which elements are potential parts of
# predicate chains
type_of_line = []
# list keeping track of dependent offsets in predicate chains
predicate_chain_dependent_offsets = []


# the following is the "main" of the script, traversing the corpus
# stream line by line
for line in fileinput.input():

 # the only delimiter we care about is </s>, in the other cases we
 # just pass through the xml elements to the print function
 if re.match("^<.*>$",line):
  if re.match("^</s",line):
   # if we are at end of sentence, we have to print and...
   process_sentence(original_sentence_lines,predicate_chain_dependent_offsets)
   # reset lists
   original_sentence_lines = []
   predicate_chain_dependent_offsets = []
   type_of_line = []

  # in all cases, we need to print the current line
  sys.stdout.write(line)
  continue # done processing a line with an xml element

 # now processing lines containing actual words, recall that format is
 # tab-delimited:
 # word lemma pos offset head_offset dep_rel

 # we keep a copy of original sentence in an array, line by line
 original_sentence_lines.append(line)

 # by default, the predicate_chain_dependent_offsets is 0, non-0 only
 # for actual items in the predicate chain (updated LATER as we find
 # dependents)
 predicate_chain_dependent_offsets.append(0)

 # we should record if this is a potential predicate chain element and
 # in this case update the dependent offset of its head, if the latter
 # is also a predicate chain element
 matchedverb = re.match("^[^\t]+\t[^\t]+\t(JJ|MD|V)[^\t]*\t([^\t]+)\t([^\t]+)\t([^\t]+)$",line)
 if matchedverb:
  curr_offset = int(matchedverb.group(2))
  head_offset = int(matchedverb.group(3))
  dep_rel = matchedverb.group(4).strip('\n')
  type_of_line.append("p")
  # if the element has a real head (not root!), the head of the
  # element is also a potential predicate chain element, it precedes
  # the current element linearly, and the relation is not COORD, PRN,
  # OBJ nor ADV, update the head dependent info
  if (head_offset>0) and (head_offset<curr_offset) and (type_of_line[head_offset-1] == "p") and (not re.match("^(PRN|OBJ|ADV|COORD)",dep_rel)):
   predicate_chain_dependent_offsets[head_offset-1]=curr_offset
 else:
 # in all other cases, we are not interested in the category of the item
  type_of_line.append("o")

fileinput.close()



