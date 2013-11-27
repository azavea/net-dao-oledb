@echo off

for %%f in (nupkg\*) do (

    REM Does this filename contain ".symbols.nupkg"?

    echo.%%f | findstr /C:".symbols.nupkg" 1>nul

    if errorlevel 1 (

        echo.
        echo ---- Pushing package to public repository
        .nuget\NuGet.exe push %%f
        if errorlevel 1 exit

        echo.
        echo ---- Pushing package to local repository
        .nuget\NuGet.exe push %%f -Source http://nupeek.internal.azavea.com/

    ) else (

        echo.
        echo ---- Pushing symbols to local repository
        .nuget\NuGet.exe push %%f -Source http://nupeek.internal.azavea.com/

    )

)

