[int]$LinesInFile = 0
$reader = New-Object IO.StreamReader 'sample_incremental.csv'
 while($reader.ReadLine() -ne $null){ $LinesInFile++ }