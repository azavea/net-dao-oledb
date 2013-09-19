echo off
set pkg_name=%1
echo on
.nuget\NuGet.exe push %pkg_name%
.nuget\NuGet.exe push %pkg_name% -Source http://nupeek.internal.azavea.com