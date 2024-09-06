graph TD;
    A["TerraTransfer Collect Logger Script"] --> B["TerraTransfer API (api_key)"];
    B --> C["Daily Data in Folder"];
    C --> D["Aggregate Script"];
    D --> E["Monthly Aggregated Data"];
    D --> F["Yearly Aggregated Data"];
    D --> G["Summary Data"];
