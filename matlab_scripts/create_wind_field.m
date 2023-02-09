import mlreportgen.dom.*

% T = readtable(path_to_file + filename, "ReadRowNames", true, "ReadVariableNames", true);
% a = T("46.74", "x5_02");
% b = T(179, 449);

path_to_data = "";
filename = "icon-d2_germany_regular-lat-lon_model-level_2022112500_000_2_u.csv";

% Lat-Lon box of germany
% LON: 5°E - 15°E (500 elements)
% LAT: 47°N - 55°N (400 elements)

% 5°E = Col 448
% 14.98°E = Col 947
% 47°N = Row 192
% 54.98°N = Row 591

number_of_longitude_elements = 500;
number_of_latitude_elemtents = 400;
number_of_heights = 29;
number_of_hours = 48;

function wind_field = get_freezed_wind_field(date, hour, field)
    wind_field = zeros(number_of_longitude_elements, number_of_latitude_elemtents, number_of_heights, "single");

    for idx = 1:number_of_heights
        filename = sprintf("icon-d2_germany_regular-lat-lon_model-level_%s_0%02d_%d_%s.csv", date, hour, idx, field);
        mat = readtable(path_to_data + filename, "ReadRowNames", true, "ReadVariableNames", true);
        wind_field(:, :, idx) = mat(448:947, 192:591);
    end
end

hours_u = OrderedList([]);
hours_v = OrderedList([]);
hours_w = OrderedList([]);
for hour = 0:number_of_hours
    append(get_freezed_wind_field("2022112500", hour, "u"), hours_u);
    append(get_freezed_wind_field("2022112500", hour, "v"), hours_u);
    append(get_freezed_wind_field("2022112500", hour, "w"), hours_u);
end
