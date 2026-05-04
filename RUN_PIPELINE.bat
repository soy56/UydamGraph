@echo off
echo ======================================================================
echo   UdyamGraph Pipeline - Running with Mock Data
echo ======================================================================
echo.

python pipeline.py --mock --output data/mock_results.json

echo.
echo ======================================================================
echo   Pipeline Complete!
echo   Results saved to: data/mock_results.json
echo ======================================================================
echo.
pause
