1. **TerraTransfer Collect Logger Script**:
   - Calls the **TerraTransfer API** using an `api_key`.
   - Retrieves and writes the **daily data** into a folder per device.

2. **Aggregate Script**:
   - Processes the **daily data**.
   - Aggregates the data into:
     - A **monthly data folder**.
     - A **yearly data folder**.
     - A **summary file** with all data per device.
