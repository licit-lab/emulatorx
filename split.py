splitLen = 39949         # 50000 lines per file
outputBase = 'links' # output.1.txt, output.2.txt, etc.

# This is shorthand and not friendly with memory
# on very large files (Sean Cavanagh), but it works.
#319592 observations
input = open('all_links.csv', 'r').read().split('\n')
at = 1
for lines in range(0, len(input), splitLen):
    # First, get the list slice
    outputData = input[lines:lines+splitLen]

    # Now open the output file, join the new slice with newlines
    # and write it out. Then close the file.
    output = open(outputBase + str(at) + '.csv', 'w')
    output.write('\n'.join(outputData))
    output.close()

    # Increment the counter
    at += 1