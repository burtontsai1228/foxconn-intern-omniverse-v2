import json
import math
import omni.ext
import omni.ui as ui
import omni.usd
import omni.kit.app
from pxr import UsdGeom, Gf
from kafka import KafkaConsumer

BROKER = "localhost:9092"
JOINT_TOPIC = "robot_states"
POSE_TOPIC = "robot_pose"

INVERT = {
    "left_3",
    "right_3",
    "head_pitch",
}

REF_X = 5.0
REF_Y = 3.0
REF_YAW = 1.57


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

    def reset(self):
        self.offset_x = 0.0
        self.offset_y = 0.0
        self.alpha = 0.0

    def describe(self):
        return (f"alpha={math.degrees(self.alpha):.1f}deg "
                f"offset=({self.offset_x:.3f}, {self.offset_y:.3f})")


class MyExtension(omni.ext.IExt):
    def on_startup(self, _ext_id):
        self._joint_consumer = None
        self._pose_consumer = None
        self._sub = None
        self._window = None
        self._rot_ops = {}
        self._base_translate = None
        self._base_rotate = None
        self._jack_op = None
        self._aligner = MapAligner()
        self._latest_pose = None

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
                self._base_translate = ops[0]
                self._base_rotate = ops[2]
                print("[burton.spawn] base_link  -> translate[0] + rotateXYZ[2]")
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

        self._joint_consumer = KafkaConsumer(
            JOINT_TOPIC,
            bootstrap_servers=BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )
        print(f"[burton.spawn] connected to {BROKER}, topic '{JOINT_TOPIC}'")

        self._pose_consumer = KafkaConsumer(
            POSE_TOPIC,
            bootstrap_servers=BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )
        print(f"[burton.spawn] connected to {BROKER}, topic '{POSE_TOPIC}'")

        self._sub = (
            omni.kit.app.get_app()
            .get_update_event_stream()
            .create_subscription_to_pop(self._on_update, name="burton.spawn kafka")
        )

        self._window = ui.Window("Fiibot Calibration", width=320, height=140)
        with self._window.frame:
            with ui.VStack(spacing=6):
                ui.Label("Drive robot to reference point, then:")
                ui.Button("Calibrate Here", clicked_fn=self._calibrate_here)
                ui.Button("Reset", clicked_fn=self._reset)

    def _calibrate_here(self):
        if self._latest_pose is None:
            print("[burton.spawn] no pose received yet")
            return
        mx, my, mth = self._latest_pose
        self._aligner.calibrate(mx, my, mth, REF_X, REF_Y, REF_YAW)
        print(f"[burton.spawn] calibrated at map({mx:.2f}, {my:.2f}, {mth:.2f})")
        print(f"[burton.spawn]   {self._aligner.describe()}")

    def _reset(self):
        self._aligner.reset()
        print("[burton.spawn] calibration reset")

    def _on_update(self, _e):
        self._update_joints()
        self._update_pose()

    def _update_joints(self):
        if not self._joint_consumer:
            return

        records = self._joint_consumer.poll(timeout_ms=0)
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

    def _update_pose(self):
        if not self._pose_consumer or self._base_translate is None:
            return

        records = self._pose_consumer.poll(timeout_ms=0)
        if not records:
            return

        latest = None
        for _tp, msgs in records.items():
            if msgs:
                latest = msgs[-1].value
        if not latest:
            return

        x = latest.get("x")
        y = latest.get("y")
        theta = latest.get("theta")
        if x is None or y is None or theta is None:
            return

        self._latest_pose = (float(x), float(y), float(theta))

        X, Y, yaw = self._aligner.to_scene(float(x), float(y), float(theta))
        self._base_translate.Set(Gf.Vec3d(X, Y, 0))
        self._base_rotate.Set(Gf.Vec3d(0, 0, math.degrees(yaw)))

    def on_shutdown(self):
        if self._sub:
            self._sub.unsubscribe()
            self._sub = None
        if self._joint_consumer:
            self._joint_consumer.close()
            self._joint_consumer = None
        if self._pose_consumer:
            self._pose_consumer.close()
            self._pose_consumer = None
        self._window = None