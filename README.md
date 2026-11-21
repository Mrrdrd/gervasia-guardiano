# Gervàsia guardiano

A Home Assistant add-on that watches every request handled by the voice pipelines
(Assist) and asks a Claude model whether the fast assistant served the person well.
If not, it can look around the house (states, logbook, history, traces, system log)
and step in: speak a short answer from the same speaker, call a service, notify, or
add an item to a to-do list.

It starts in **shadow mode**: it only writes what it would do, to
`/share/guardiano/decisioni.jsonl` and to `sensor.guardiano_ultima_decisione`.

## Setup
1. Add this repository in Settings, Add-ons, Add-on store, Repositories.
2. Install "Gervàsia guardiano".
3. Put your Anthropic API key in the add-on configuration.
4. Write the guardian's instructions in `/homeassistant/guardiano/istruzioni.txt`
   and the per-device settings in `/homeassistant/guardiano/impostazioni.json`.
5. Start it. Turn `shadow_mode` off when its decisions look right.

Home-specific instructions and settings never live in this repository.


## Chi parla (chi_parla/)

Speech to text for Gervàsia that also recognises who is speaking. Home Assistant sends each request's audio here over Wyoming; it is streamed on to Home Assistant Cloud speech to text as before, and a local CAM++ model (3D-Speaker) compares the voice with the family voiceprints, writing "Name|alta|time" to input_text.chi_parla before the transcript returns. Voiceprints are kept only on the HP in /share/chi_parla/voiceprints.json, never in this repository.
