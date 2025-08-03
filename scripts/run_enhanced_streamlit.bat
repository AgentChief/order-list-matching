@echo off
cd /d "c:\Users\AUKALATC01\Dev\order-list-matching"
echo Starting Enhanced Streamlit App with Layer 1 and Layer 2 matching...
streamlit run src\ui\enhanced_streamlit_app.py --server.port 8502
pause
