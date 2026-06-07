





"""
Visualization module for collision trajectories using Folium maps.
Updated version with Esri Satellite map and glow-style trajectories.
"""

import math
from typing import List, Dict, Tuple

import folium
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    OUTPUT_DIR, TRAJECTORY_WINDOW_MIN, MAP_ZOOM_START,
    TRAJECTORY_COLOR_A, TRAJECTORY_COLOR_B, ICON_COLOR_START,
    ICON_COLOR_END, ICON_COLOR_COLLISION
)
from utils import setup_logger, classify_colreg, format_collision_report, get_output_path


logger = setup_logger(__name__)


class CollisionVisualizer:
    """Generates visualization maps for collision events (Esri Satellite version)."""
    
    def __init__(self, df_filtered: DataFrame):
        self.df_filtered = df_filtered
    
    # def visualize_all(self, final_result) -> List[str]:
    #     output_files = []
        
    #     TARGET_MMSI_A = 219021240
    #     TARGET_MMSI_B = 232018267
        
    #     if isinstance(final_result, pd.DataFrame):
    #         df = final_result.copy()
    #     else:
    #         df = final_result.toPandas()
        
    #     target_collision = df[
    #         ((df['MMSI_A'] == TARGET_MMSI_A) & (df['MMSI_B'] == TARGET_MMSI_B)) |
    #         ((df['MMSI_A'] == TARGET_MMSI_B) & (df['MMSI_B'] == TARGET_MMSI_A))
    #     ]
        
    #     if not target_collision.empty:
    #         logger.info(f"Found target collision: MMSI {TARGET_MMSI_A} <-> {TARGET_MMSI_B}")
    #         all_events = [target_collision.iloc[0].to_dict()]
    #     else:
    #         df_sorted = df.sort_values("vessel_distance_nm").drop_duplicates(
    #             subset=["MMSI_A", "MMSI_B"]
    #         )
    #         all_events = [row.to_dict() for _, row in df_sorted.head(5).iterrows()]
        
    #     logger.info(f"Generating visualization for {len(all_events)} collision(s)")
        
    #     for i, collision_event in enumerate(all_events):
    #         try:
    #             output_path = self._visualize_collision(collision_event, i + 1, len(all_events))
    #             output_files.append(output_path)
    #         except Exception as e:
    #             logger.error(f"Failed to visualize collision {i+1}: {e}")
        
    #     return output_files

    def visualize_all(self, final_result) -> List[str]:
        """
        Generate ONE combined map containing ALL priority collision pairs.
        """
        output_files = []

        # --- PRIORITY MMSI PAIRS (your two pairs) ---
        PRIORITY_PAIRS = [
            (219021240, 232018267),   # Pair 1
            (219019287, 219021428),   # Pair 2
        ]

        # Convert Spark → Pandas if needed
        if isinstance(final_result, pd.DataFrame):
            df = final_result.copy()
        else:
            df = final_result.toPandas()

        # --- FIND ALL PRIORITY COLLISIONS ---
        collision_events = []
        for a, b in PRIORITY_PAIRS:
            hit = df[
                ((df["MMSI_A"] == a) & (df["MMSI_B"] == b)) |
                ((df["MMSI_A"] == b) & (df["MMSI_B"] == a))
            ]
            if not hit.empty:
                logger.info(f"Found priority collision: {a} <-> {b}")
                collision_events.append(hit.iloc[0].to_dict())

        # If none found → fallback to closest encounters
        if not collision_events:
            df_sorted = df.sort_values("vessel_distance_nm").drop_duplicates(
                subset=["MMSI_A", "MMSI_B"]
            )
            collision_events = [row.to_dict() for _, row in df_sorted.head(5).iterrows()]

        logger.info(f"Generating ONE combined map for {len(collision_events)} collision(s)")

        # --- CREATE BASE MAP CENTERED ON FIRST COLLISION ---
        first = collision_events[0]
        m = self._create_map(float(first["Lat_A"]), float(first["Lon_A"]))

        # --- DRAW ALL COLLISIONS ON SAME MAP ---
        for event in collision_events:
            mmsi_a = int(event["MMSI_A"])
            mmsi_b = int(event["MMSI_B"])
            collision_time = str(event["first_ts"])

            traj_a, traj_b = self._extract_trajectories(mmsi_a, mmsi_b, event["first_ts"])

            # Add trajectories
            self._add_trajectories(m, traj_a, traj_b,
                                event.get("Name_A", f"MMSI {mmsi_a}"),
                                event.get("Name_B", f"MMSI {mmsi_b}"))

            # Add markers
            self._add_markers(
                m, traj_a, traj_b,
                event.get("Name_A", f"MMSI {mmsi_a}"),
                event.get("Name_B", f"MMSI {mmsi_b}"),
                float(event["Lat_A"]), float(event["Lon_A"]),
                collision_time
            )

        # --- ADD LEGEND FOR ALL COLLISIONS ---
        legend_html = "<div style='position: fixed; bottom: 30px; left: 30px; " \
                    "z-index: 1000; background-color: rgba(255,255,255,0.85); " \
                    "padding: 12px; border-radius: 6px; border: 1px solid #ccc; " \
                    "font-size: 13px; font-family: monospace;'>"

        legend_html += "<b>Combined Collision Map</b><br>"

        for event in collision_events:
            legend_html += (
                f"<span style='color:#00eaff'>●</span> "
                f"{event.get('Name_A', event['MMSI_A'])} (MMSI {event['MMSI_A']})<br>"
                f"<span style='color:#ff00aa'>●</span> "
                f"{event.get('Name_B', event['MMSI_B'])} (MMSI {event['MMSI_B']})<br>"
                "<br>"
            )

        legend_html += "</div>"
        m.get_root().html.add_child(folium.Element(legend_html))

        # --- SAVE ONE HTML FILE ---
        output_path = get_output_path("combined_collision_map.html")
        m.save(output_path)
        logger.info(f"Combined map saved: {output_path}")

        return [output_path]


    
    
    # ───────────────────────────────────────────────────────────────
    # COLLISION VISUALIZATION
    # ───────────────────────────────────────────────────────────────
    
    def _visualize_collision(self, collision_event: dict, event_num: int,
                            total_events: int) -> str:
        
        mmsi_a = int(collision_event.get("MMSI_A", 0))
        mmsi_b = int(collision_event.get("MMSI_B", 0))
        collision_time = str(collision_event.get("first_ts", "Unknown"))
        vessel_name_a = collision_event.get("Name_A", f"MMSI {mmsi_a}")
        vessel_name_b = collision_event.get("Name_B", f"MMSI {mmsi_b}")
        collision_lat = float(collision_event.get("Lat_A", 0))
        collision_lon = float(collision_event.get("Lon_A", 0))
        
        try:
            scenario = classify_colreg(
                collision_lat, collision_lon,
                float(collision_event.get("HDG_A", 0)),
                float(collision_event.get("Lat_B", 0)), float(collision_event.get("Lon_B", 0))
            )
        except:
            scenario = "Unknown"
        
        collision_data = {
            "MMSI_A": mmsi_a,
            "MMSI_B": mmsi_b,
            "Name_A": vessel_name_a,
            "Name_B": vessel_name_b,
            "first_ts": collision_time,
            "Lat_A": collision_lat,
            "Lon_A": collision_lon,
            "vessel_distance_nm": collision_event.get("vessel_distance_nm", 0),
            "DCPA": collision_event.get("DCPA", "N/A"),
            "TCPA": collision_event.get("TCPA", "N/A"),
            "SOG_A": collision_event.get("SOG_A", 0),
            "SOG_B": collision_event.get("SOG_B", 0),
            "ShipType_A": collision_event.get("ShipType_A", "Unknown"),
            "ShipType_B": collision_event.get("ShipType_B", "Unknown"),
            "duration_seconds": collision_event.get("duration_seconds", 0),
            "ping_count": collision_event.get("ping_count", 0),
            "scenario": scenario
        }
        
        logger.info(format_collision_report(collision_data))
        logger.info(f"Collision {event_num}/{total_events}: {vessel_name_a} ↔ {vessel_name_b}")
        
        traj_a, traj_b = self._extract_trajectories(mmsi_a, mmsi_b, collision_event.get("first_ts", ""))
        
        logger.info(f"  Vessel A trajectory: {len(traj_a)} points")
        logger.info(f"  Vessel B trajectory: {len(traj_b)} points")
        
        m = self._create_map(collision_lat, collision_lon)
        
        self._add_trajectories(m, traj_a, traj_b, vessel_name_a, vessel_name_b)
        self._add_markers(m, traj_a, traj_b, vessel_name_a, vessel_name_b,
                         collision_lat, collision_lon, collision_time)
        self._add_legend(m, vessel_name_a, vessel_name_b, mmsi_a, mmsi_b, collision_time)
        
        output_path = get_output_path(
            f"collision_map_{mmsi_a}_{mmsi_b}.html"
        )
        m.save(output_path)
        logger.info(f"  Map saved: {output_path}")
        
        return output_path
    
    # ───────────────────────────────────────────────────────────────
    # TRAJECTORY EXTRACTION
    # ───────────────────────────────────────────────────────────────
    
    def _extract_trajectories(self, mmsi_a: int, mmsi_b: int,
                             collision_time) -> Tuple[pd.DataFrame, pd.DataFrame]:
        
        trajectory = self.df_filtered.filter(
            (F.col("MMSI").isin([mmsi_a, mmsi_b])) &
            (F.col("Timestamp") >= F.lit(collision_time) - F.expr(f"INTERVAL {TRAJECTORY_WINDOW_MIN} MINUTES")) &
            (F.col("Timestamp") <= F.lit(collision_time) + F.expr(f"INTERVAL {TRAJECTORY_WINDOW_MIN} MINUTES"))
        ).select("MMSI", "Name", "Timestamp", "Latitude", "Longitude", "SOG", "COG") \
         .orderBy("Timestamp")
        
        traj_pd = trajectory.toPandas()
        traj_a = traj_pd[traj_pd["MMSI"] == mmsi_a].sort_values("Timestamp").reset_index(drop=True)
        traj_b = traj_pd[traj_pd["MMSI"] == mmsi_b].sort_values("Timestamp").reset_index(drop=True)
        
        return traj_a, traj_b
    
    # ───────────────────────────────────────────────────────────────
    # MAP CREATION — ESRI SATELLITE
    # ───────────────────────────────────────────────────────────────
    
    @staticmethod
    def _create_map(center_lat: float, center_lon: float) -> folium.Map:
        """Create bright satellite map with nautical overlay."""
        
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=MAP_ZOOM_START,
            tiles=None
        )

        # High-resolution satellite imagery
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/"
                  "World_Imagery/MapServer/tile/{z}/{y}/{x}",
            attr="Esri Satellite",
            name="Esri Satellite",
        ).add_to(m)

        # Nautical chart overlay
        folium.TileLayer(
            tiles="https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png",
            attr="OpenSeaMap",
            name="Nautical",
            overlay=True,
            opacity=0.8
        ).add_to(m)

        folium.LayerControl().add_to(m)
        return m
    
    # ───────────────────────────────────────────────────────────────
    # TRAJECTORIES + MARKERS — GLOW STYLE
    # ───────────────────────────────────────────────────────────────
    
    @staticmethod
    def _add_trajectories(m: folium.Map, traj_a: pd.DataFrame, traj_b: pd.DataFrame,
                         name_a: str, name_b: str) -> None:
        
        def glow_polyline(coords, color):
            # Outer glow
            folium.PolyLine(coords, color=color, weight=10, opacity=0.25).add_to(m)
            # Mid glow
            folium.PolyLine(coords, color=color, weight=6, opacity=0.6).add_to(m)
            # Core line
            folium.PolyLine(coords, color=color, weight=3, opacity=1).add_to(m)

        if len(traj_a) > 0:
            coords_a = list(zip(traj_a["Latitude"], traj_a["Longitude"]))
            glow_polyline(coords_a, "#00eaff")  # cyan glow
        
        if len(traj_b) > 0:
            coords_b = list(zip(traj_b["Latitude"], traj_b["Longitude"]))
            glow_polyline(coords_b, "#ff00aa")  # magenta glow
    
    @staticmethod
    def _add_markers(m: folium.Map, traj_a: pd.DataFrame, traj_b: pd.DataFrame,
                    name_a: str, name_b: str, collision_lat: float, collision_lon: float,
                    collision_time: str) -> None:
        
        if len(traj_a) > 0:
            coords_a = list(zip(traj_a["Latitude"], traj_a["Longitude"]))
            folium.CircleMarker(
                coords_a[0], radius=6,
                color=ICON_COLOR_START, fill=True,
                popup=f"{name_a} — start (-10 min)"
            ).add_to(m)
            folium.CircleMarker(
                coords_a[-1], radius=6,
                color=ICON_COLOR_END, fill=True,
                popup=f"{name_a} — end (+10 min)"
            ).add_to(m)
        
        if len(traj_b) > 0:
            coords_b = list(zip(traj_b["Latitude"], traj_b["Longitude"]))
            folium.CircleMarker(
                coords_b[0], radius=6,
                color="blue", fill=True,
                popup=f"{name_b} — start (-10 min)"
            ).add_to(m)
            folium.CircleMarker(
                coords_b[-1], radius=6,
                color="darkblue", fill=True,
                popup=f"{name_b} — end (+10 min)"
            ).add_to(m)
        
        folium.Marker(
            [collision_lat, collision_lon],
            popup=f"Collision — {collision_time} UTC",
            icon=folium.Icon(color=ICON_COLOR_COLLISION, icon="warning-sign", prefix="glyphicon")
        ).add_to(m)
    
    @staticmethod
    def _add_legend(m: folium.Map, name_a: str, name_b: str,
                   mmsi_a: int, mmsi_b: int, collision_time: str) -> None:
        
        legend_html = f"""
        <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
             background-color: rgba(255,255,255,0.85); padding: 12px; border-radius: 6px;
             border: 1px solid #ccc; font-size: 13px; color: black;
             font-family: monospace;">
            <b>Collision — {collision_time[:10]}</b><br>
            <span style="color:#00eaff">●</span> {name_a} (MMSI: {mmsi_a})<br>
            <span style="color:#ff00aa">●</span> {name_b} (MMSI: {mmsi_b})<br>
            <span style="color:{ICON_COLOR_COLLISION}">★</span> Collision point
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))


def visualize_collisions(df_filtered: DataFrame, final_result: DataFrame) -> List[str]:
    visualizer = CollisionVisualizer(df_filtered)
    return visualizer.visualize_all(final_result)
























































