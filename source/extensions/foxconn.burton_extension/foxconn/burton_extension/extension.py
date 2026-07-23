import json
import math
import omni.ext
import omni.usd
import omni.kit.app
from pxr import UsdGeom, Gf
from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "robot_pose"

INVERT = {
    "left_3",
    "right_3",
    "head_pitch",
}

class MapAligner:
    def __init__(self):
        self.offset_x = 0.0
        self.offset_y = 0.0
        self.alpha = 0.0

    def calibrate(self, map_x, map_y, map_theta, scene_x, scene_y, scene_yaw):
        self.alpha = scene_yaw - map_theta
        rx = map_x * math.cos(self.alpha) - map_y * math.sin(self.alpha)
        ry = map_x * math.sin(self.alpha) + map_y * math.cos(self.alpha)
        self.offset_x = scene_x - rx
        self.offset_y = scene_y - ry

    def to_scene(self, map_x, map_y, map_theta):
        X = map_x * math.cos(self.alpha) - map_y * math.sin(self.alpha) + self.offset_x
        Y = map_x * math.sin(self.alpha) + map_y * math.cos(self.alpha) + self.offset_y
        yaw = map_theta + self.alpha
        return X, Y, yaw

class MyExtension(omni.ext.IExt):
    def on_startup(self, _ext_id):
        self._consumer = None
        self._sub = None
        self._rot_ops = {}
        self._base_op = None
        self._jack_op = None
        # self._map_aligner = MapAligner()

        stage = omni.usd.get_context().get_stage()
        if stage is None:
            print("[burton.spawn] no stage open")
            return

        for prim in stage.Traverse():
            name = prim.GetName()
            if not name.endswith("_J"):
                continue

            ops = UsdGeom.Xformable(prim).GetOrderedXformOps()

            if name == "base_link_J":
                self._base_op = ops[2]
                print(f"[burton.spawn] base_link  -> {ops[2].GetOpName()} (Vec3)")
                continue

            if name == "jack_link_J":
                self._jack_op = ops[0]
                print(f"[burton.spawn] jack       -> {ops[0].GetOpName()} (Vec3)")
                continue

            drive = None
            for op in ops:
                if op.GetOpName() in ("xformOp:rotateY", "xformOp:rotateZ", "xformOp:rotateX"):
                    drive = op
                    break
            if drive is None:
                print(f"[burton.spawn] skip {name}")
                continue

            key = name.replace("_Link2_J", "").replace("_Link_J", "").replace("_link_J", "")
            self._rot_ops[key] = drive
            print(f"[burton.spawn] {key:12} -> {drive.GetOpName()}")

        print(f"[burton.spawn] cached {len(self._rot_ops)} rotation joints + base + jack")

        self._consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )
        print(f"[burton.spawn] connected to {BROKER}, topic '{TOPIC}'")

        self._sub = (
            omni.kit.app.get_app()
            .get_update_event_stream()
            .create_subscription_to_pop(self._on_update, name="burton.spawn kafka")
        )

    def _on_update(self, _e):
        if not self._consumer:
            return

        records = self._consumer.poll(timeout_ms=0)
        if not records:
            return

        latest = None
        for _tp, msgs in records.items():
            if msgs:
                latest = msgs[-1].value
        if not latest:
            return

        names = latest.get("name")
        positions = latest.get("position")
        if not names or not positions:
            return

        for joint_name, pos in zip(names, positions):
            key = joint_name.replace("_joint", "")

            if key == "jack" and self._jack_op:
                h = max(0.0, min(0.70, float(pos)))
                self._jack_op.Set(Gf.Vec3d(0, 0, h))
                continue

            op = self._rot_ops.get(key)
            if op:
                deg = math.degrees(float(pos))
                if key in INVERT:
                    deg = -deg
                op.Set(deg)

    def on_shutdown(self):
        if self._sub:
            self._sub.unsubscribe()
            self._sub = None
        if self._consumer:
            self._consumer.close()
            self._consumer = None