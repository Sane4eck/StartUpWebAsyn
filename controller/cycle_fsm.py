from __future__ import annotations

from typing import Callable, Dict, Optional

from scheme.cycle import CycleInputs, CycleTargets, State


class Hold:
    """Predicate must be True continuously for hold_s seconds."""

    def __init__(self, predicate: Callable[[CycleInputs], bool], hold_s: float):
        self.predicate = predicate
        self.hold_s = float(hold_s)
        self._t0: Optional[float] = None

    def reset(self):
        self._t0 = None

    def __call__(self, inp: CycleInputs) -> bool:
        if self.predicate(inp):
            if self._t0 is None:
                self._t0 = inp.now
                return False
            return (inp.now - self._t0) >= self.hold_s
        self._t0 = None
        return False


class CycleFSM:
    def __init__(self, states: Dict[str, State], initial: str, stop_state: str = "Stop"):
        self.states = states
        self.initial = initial
        self.stop_state = stop_state
        self.running: bool = False
        self.current: str = initial
        self._state_enter_time: float = 0.0
        self.targets = CycleTargets()
        self.last_state: Optional[str] = None
        self.last_transition_reason: Optional[str] = None

    @property
    def state(self) -> str:
        return self.current

    def start(self, inp: CycleInputs):
        self.running = True
        self._switch(inp, self.initial, reason=None)

    def stop(self, inp: CycleInputs, reason: str | None = None):
        self.running = False
        self._switch(inp, self.stop_state, reason=reason)

    def tick(self, inp: CycleInputs) -> CycleTargets:
        self.targets.meta.clear()
        self.last_transition_reason = None

        st = self.states[self.current]
        if st.on_tick:
            st.on_tick(inp, self.targets)

        if st.timeout_s is not None and st.on_timeout:
            if inp.state_t >= float(st.timeout_s):
                self._switch(inp, st.on_timeout, reason=st.timeout_reason)
                st = self.states[self.current]

        for tr in st.transitions:
            if tr.cond(inp):
                self._switch(inp, tr.next_state, reason=tr.reason)
                st = self.states[self.current]
                break

        if st.terminal:
            self.running = False

        return self.targets

    def _switch(self, inp: CycleInputs, next_state: str, reason: Optional[str]):
        self.last_state = self.current
        self.current = next_state
        self._state_enter_time = inp.now
        self.last_transition_reason = reason

        st = self.states[self.current]
        for tr in st.transitions:
            if hasattr(tr.cond, "reset"):
                try:
                    tr.cond.reset()
                except Exception:
                    pass

        if reason:
            self.targets.meta["transition_reason"] = reason

        if st.on_enter:
            st.on_enter(inp, self.targets)

    def state_time(self, now: float) -> float:
        return now - self._state_enter_time
