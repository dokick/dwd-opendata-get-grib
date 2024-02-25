filename = 'icon-d2_germany_regular-lat-lon_model-level_2024022512_000_u.bin';
fileID = fopen([filename], 'r');
DataUmatG = fread(fileID, 'double');
fclose(fileID);

number_of_height_levels = 28;  % 28 is the default for all height levels 38 until 65 (65-38+1=28)
number_of_lat_elements = 400;
number_of_lon_elements = 500;

for height = 1:number_of_height_levels
    for lat = 1:number_of_lat_elements
        for lon = 1:number_of_lon_elements
        UmatG(height, lat, lon) = DataUmatG( ...
            (height-1)*number_of_lon_elements*number_of_lat_elements + (number_of_lat_elements*(lat-1) + lon) ...
        );
        end
    end
end

csv = readtable("icon-d2_germany_regular-lat-lon_model-level_2024022512_000_38_u.csv");

disp(UmatG(1, 1:3, 1:3));

row_47_deg = 192;
column_five_deg = 448;

disp(csv( ...
    (row_47_deg + 1):(row_47_deg + 3), ...
    (column_five_deg + 1):(column_five_deg + 3) ...
));
