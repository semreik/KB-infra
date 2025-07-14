-- Metadata schema for entity resolution
CREATE TABLE supplier (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE po (
    po_id TEXT PRIMARY KEY,
    supplier_id INTEGER REFERENCES supplier(supplier_id),
    po_date DATE NOT NULL,
    delivery_date DATE,
    status TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE alias_map (
    alias_id SERIAL PRIMARY KEY,
    supplier_id INTEGER REFERENCES supplier(supplier_id),
    alias_text TEXT NOT NULL,
    source TEXT NOT NULL, -- e.g. 'email', 'po', 'invoice'
    confidence FLOAT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(alias_text, source)
);

-- Indexes
CREATE INDEX idx_supplier_name ON supplier(name);
CREATE INDEX idx_alias_text ON alias_map(alias_text);
CREATE INDEX idx_po_supplier ON po(supplier_id);
