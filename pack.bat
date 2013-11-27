@echo off
if not exist nupkg mkdir nupkg
del /q nupkg\*
echo ---- Packaging code and symbols
.nuget\NuGet.exe pack -Symbols -Prop Configuration=Release -OutputDirectory nupkg
