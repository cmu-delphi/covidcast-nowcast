"""Estimate the covariance matrix."""

from typing import List, Tuple

import numpy as np
import pandas as pd

from delphi_covidcast_nowcast.nowcast_fusion import fusion


class Locations:

    @staticmethod
    def get_real_counties_sorted(counties):
        return sorted(c for c in counties if not c.endswith('000'))

    def __init__(self):
        # load fips-population mapping
        self.fips_pop_map = pd.read_csv(
            'delphi_covidcast_nowcast/statespace/fips_pop.csv',
            dtype={'fips': str, 'pop': int})

        # avoid 'pop' reserved method name
        self.fips_pop_map.rename(columns={'pop': 'population'}, inplace=True)

        # load msa->county mapping
        self.fips_msa_map = pd.read_csv(
            'delphi_covidcast_nowcast/statespace/fips_msa_table.csv', dtype=str)
        self.counties_from_msa = self.fips_msa_map.groupby('msa')['fips'].apply(list)

        # load state->county mapping
        fips_state_map = pd.read_csv(
            'delphi_covidcast_nowcast/statespace/fips_state_table.csv', dtype=str)

        # attach population
        self.fips_state_map = fips_state_map.merge(self.fips_pop_map, how="left",
                                                   on="fips")
        self.counties_from_state = fips_state_map.groupby('state_id')['fips'].apply(
            Locations.get_real_counties_sorted)

        # fastering population filter map
        self.fips_pop_map_filt = self.fips_pop_map.set_index('fips')

    def filter_population_by_state(self, state_id):
        """Pre-filter population map to only include fips inside given state."""
        fips_in_state = self.counties_from_state.loc[state_id]
        self.fips_pop_map_filt = self.fips_pop_map_filt[
            self.fips_pop_map_filt.index.isin(fips_in_state)]

    def state_list(self):
        return sorted(set(self.fips_state_map.state_id))

    def msa_list(self):
        return sorted(set(self.counties_from_msa.index))

    def county_list(self):
        return sorted(set(self.fips_state_map.fips))

    def get_county_pop(self, fips):
        return self.fips_pop_map_filt.loc[fips].population

    def get_counties_in_state(self, state_id):
        return self.counties_from_state.loc[state_id]

    def get_counties_in_msa(self, msa_id):
        return self.counties_from_msa.loc[msa_id]

    def get_msas_in_state(self, state_id):
        counties_in_state = self.get_counties_in_state(state_id)
        return sorted(self.fips_msa_map[
                          self.fips_msa_map.fips.isin(counties_in_state)].msa.unique())


def generate_statespace(state_id: str, input_location_types: List[tuple]) -> \
        Tuple[np.ndarray, np.ndarray, List]:
    """
    Generate W and H matrices.

    Parameters
    ----------
    state_id
        string with US state location id, e.g. 'pa'
    input_location_types
        tuple of (location_id, location_type) for the input sensors.

    Returns
    -------
        Full rank matrices W and H, and list of output locations
    """

    loc_map = Locations()
    loc_map.filter_population_by_state(state_id)

    # list of all locations: state, msa, county
    all_location_types = [(state_id, 'state')]
    for loc in loc_map.get_msas_in_state(state_id):
        all_location_types.append((loc, 'msa'))
    for loc in loc_map.get_counties_in_state(state_id):
        all_location_types.append((loc, 'county'))

    # list of all atoms (counties)
    atom_list = loc_map.get_counties_in_state(state_id)

    def get_weight_row(location, location_type, atoms):
        """
        Calculate the population weights for a sensor at the given location.

        This approach will always create rows that sum to 1, even if atoms are
        missing or incomplete. Alternative approach tried in colab/deconvolution.ipynb.
        """

        total_population = 0
        atom_populations = []

        # todo: cleanup
        if location_type == 'county':
            for atom in atoms:
                if atom == location:
                    population = loc_map.get_county_pop(atom)
                else:
                    population = 0
                total_population += population
                atom_populations.append(population)

        elif location_type == 'msa':
            atoms_in_msa = loc_map.get_counties_in_msa(location)
            for atom in atoms:
                if atom in atoms_in_msa:
                    population = loc_map.get_county_pop(atom)
                else:
                    population = 0
                total_population += population
                atom_populations.append(population)

        elif location_type == 'state':
            atoms_in_state = loc_map.get_counties_in_state(location)
            for atom in atoms:
                if atom in atoms_in_state:
                    population = loc_map.get_county_pop(atom)
                else:
                    population = 0
                total_population += population
                atom_populations.append(population)

        # sanity check
        if total_population == 0:
            raise Exception(('location has no constituent atoms', location))

        ## fractional seems to be slower? is numerical performance much different?
        # return list of fractional populations
        # get_fraction = lambda pop: Fraction(pop, total_population)
        get_fraction = lambda pop: pop / total_population
        return list(map(get_fraction, atom_populations))

    def get_weight_matrix(location_types, atoms):
        get_row = lambda loc: get_weight_row(loc[0], loc[1], atoms)
        return np.array(list(map(get_row, location_types)))

    H0 = get_weight_matrix(input_location_types, atom_list)
    W0 = get_weight_matrix(all_location_types, atom_list)

    # get H and W from H0 and W0
    print('coalesce statespace...')
    H, W, output_idx = fusion.determine_statespace(H0, W0)
    output_locations = [all_location_types[i] for i in output_idx]

    return H, W, output_locations
