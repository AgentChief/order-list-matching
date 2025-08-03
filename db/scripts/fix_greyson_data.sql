-- Update sample orders to match real GREYSON PO 4755 data
-- This creates realistic test data for exact matching

-- Clear existing sample data
DELETE FROM int_orders_extended WHERE customer_name = 'GREYSON' AND po_number = '4755';

-- Insert realistic matching orders based on actual shipment data
INSERT INTO int_orders_extended (
    order_id, customer_name, po_number, style_code, color_code, 
    color_description, style_color_key, quantity, order_date
) VALUES 
-- Exact matches for testing (quantities slightly different to test QTY mismatch logic)
('ORD-G4755-R01', 'GREYSON', '4755', 'LFA24B05', '476', 'WOLF BLUE', 'LFA24B05-476 - WOLF BLUE', 350, '2025-06-01'),
('ORD-G4755-R02', 'GREYSON', '4755', 'LFA24B21', '001', 'SHEPHERD', 'LFA24B21-001 - SHEPHERD', 150, '2025-06-01'),
('ORD-G4755-R03', 'GREYSON', '4755', 'LFA24I21', '001', 'SHEPHERD', 'LFA24I21-001 - SHEPHERD', 150, '2025-06-01'),
('ORD-G4755-R04', 'GREYSON', '4755', 'LFA24K77', '100', 'ARCTIC', 'LFA24K77-100 - ARCTIC', 200, '2025-06-01'),
('ORD-G4755-R05', 'GREYSON', '4755', 'LFA25B09A', '051', 'SMOKE HEATHER', 'LFA25B09A-051 - SMOKE HEATHER', 600, '2025-06-01'),

-- Some fuzzy matches (similar but not exact style codes)
('ORD-G4755-F01', 'GREYSON', '4755', 'LFA25B26', '531', 'WISTERIA', 'LFA25B26-531 - WISTERIA', 100, '2025-06-01'),
('ORD-G4755-F02', 'GREYSON', '4755', 'LFA25B28', '531', 'WISTERIA', 'LFA25B28-531 - WISTERIA', 80, '2025-06-01'),
('ORD-G4755-F03', 'GREYSON', '4755', 'LSP24K59', '100', 'ARCTIC', 'LSP24K59-100 - ARCTIC', 75, '2025-06-01'),

-- Some unmatched orders (don't exist in shipments)
('ORD-G4755-U01', 'GREYSON', '4755', 'TEST001', 'BLK', 'BLACK', 'TEST001-BLK', 50, '2025-06-01'),
('ORD-G4755-U02', 'GREYSON', '4755', 'TEST002', 'WHT', 'WHITE', 'TEST002-WHT', 25, '2025-06-01');

PRINT 'Updated GREYSON PO 4755 orders with realistic matching data';
