# React + Electron Order Matching Application Setup

## 🎯 Overview

This is a modern React + Electron desktop application for the Order-Shipment Matching System (TASK013). It provides a sophisticated interface for managing order reconciliation with full CRUD capabilities.

## 🏗️ Architecture

- **Frontend**: React 18 + Ant Design UI
- **Desktop**: Electron for Windows deployment
- **Backend**: Python Flask API server
- **Database**: SQL Server with enhanced matching engine

## 🚀 Quick Start

### Prerequisites

- Node.js 16+ and Yarn
- Python 3.9+ with required packages
- SQL Server database access

### 1. Start Backend API Server

```bash
cd /app/backend
python api_server.py
```

The API server will run on `http://localhost:8001`

### 2. Start React Development Server

```bash
cd /app/frontend
yarn start
```

The React app will run on `http://localhost:3000`

### 3. Run as Electron Desktop App

```bash
cd /app/frontend
yarn electron-dev
```

This starts both React and Electron in development mode.

## 📦 Building for Production

### Build React App

```bash
cd /app/frontend
yarn build
```

### Build Electron Desktop App

```bash
cd /app/frontend
yarn electron-pack
```

This creates a Windows installer in the `dist/` directory.

## 🎨 Features

### Dashboard
- Real-time system metrics
- Layer performance visualization
- Customer activity tracking
- System health monitoring

### Data Viewer
- Browse all database tables
- CRUD operations on records
- Advanced filtering and search
- Export to CSV functionality

### Queue Manager
- HITL review queue management
- Bulk approve/reject operations
- Priority-based sorting
- Real-time status updates

### HITL Review Center
- Human-in-the-loop matching review
- Detailed match analysis
- Confidence scoring visualization
- Justification tracking

### Matching Engine
- Interactive 4-layer matching execution
- Real-time progress tracking
- Layer performance breakdown
- Execution history

### Procedure Runner
- Execute stored procedures
- Parameter input validation
- Execution history tracking
- Quick action shortcuts

### Analytics
- Performance trend analysis
- Layer efficiency metrics
- Customer performance insights
- System optimization recommendations

## 🔧 Configuration

### Environment Variables

Create `/app/frontend/.env`:
```env
REACT_APP_BACKEND_URL=http://localhost:8001/api
```

### Database Configuration

Ensure your `auth_helper.py` is configured with proper database credentials.

## 📁 Project Structure

```
/app/frontend/
├── public/
│   ├── electron.js          # Electron main process
│   └── index.html          # HTML template
├── src/
│   ├── components/         # React components
│   │   ├── Dashboard.js
│   │   ├── DataViewer.js
│   │   ├── QueueManager.js
│   │   ├── HitlReview.js
│   │   ├── MatchingEngine.js
│   │   ├── ProcedureRunner.js
│   │   └── Analytics.js
│   ├── services/
│   │   └── ApiService.js   # API client
│   ├── App.js             # Main application
│   └── index.js           # Entry point
└── package.json           # Dependencies and scripts

/app/backend/
└── api_server.py          # Flask API server
```

## 🚀 Deployment Options

### Option 1: Standalone Desktop App

1. Build the React app: `yarn build`
2. Package with Electron: `yarn electron-pack`
3. Distribute the installer

### Option 2: Client-Server Setup

1. Deploy Flask API server on a server
2. Update `REACT_APP_BACKEND_URL` to server URL
3. Build and distribute desktop app

### Option 3: Local Development

1. Run both backend and frontend locally
2. Use for development and testing

## 🔍 Troubleshooting

### Common Issues

**Blank Screen**: 
- Check if backend API is running on port 8001
- Verify database connection strings
- Check browser console for errors

**Database Connection Errors**:
- Ensure SQL Server is accessible
- Verify credentials in `auth_helper.py`
- Check firewall settings

**Build Issues**:
- Clear node_modules: `rm -rf node_modules && yarn install`
- Clear Electron cache: `yarn electron-builder clean`

### Debug Mode

Enable debugging in Electron:
```bash
# Set environment variable
DEBUG=true yarn electron-dev
```

This opens DevTools automatically.

## 📋 API Endpoints

The backend provides comprehensive REST API:

- `GET /api/system/status` - System status
- `GET /api/dashboard/overview` - Dashboard data
- `GET /api/data/{table}` - Table data with pagination
- `GET /api/queue/hitl` - HITL review queue
- `POST /api/matching/run` - Execute matching
- `POST /api/procedures/execute` - Run stored procedures
- `GET /api/analytics/*` - Analytics data

## 🎯 Key Benefits

### For Users
- **Modern Interface**: Clean, responsive design
- **Desktop Experience**: Native Windows application feel
- **Real-time Updates**: Live system monitoring
- **Bulk Operations**: Efficient queue management
- **Visual Analytics**: Performance insights

### For Administrators
- **Database Access**: Full CRUD capabilities
- **Procedure Execution**: Direct database operations
- **System Monitoring**: Performance tracking
- **Export Functions**: Data extraction tools

## 🔄 Future Enhancements

- Real-time WebSocket updates
- Advanced filtering and search
- Custom dashboard widgets
- Report generation and scheduling
- User authentication and roles
- Multi-database support

## 📞 Support

For issues or questions:

1. Check the console logs in both React and Electron
2. Verify API server is running and accessible
3. Test database connectivity independently
4. Review the troubleshooting section above

## 🎉 Success!

You now have a modern, professional desktop application for order-shipment matching with:

✅ **Full Database Access** - View, edit, create, delete records
✅ **Queue Management** - Efficient HITL review workflow  
✅ **Matching Engine** - Interactive execution with real-time feedback
✅ **Analytics Dashboard** - Performance insights and trends
✅ **Stored Procedures** - Direct database operations
✅ **Windows Deployment** - Professional desktop application

The application successfully replaces the Streamlit interface with a much more capable and user-friendly solution!